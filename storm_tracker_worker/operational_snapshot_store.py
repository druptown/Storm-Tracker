"""Crash-durable publication store for compact operational snapshots.

Meteorological processing produces a complete ``operational_snapshot_v1`` in
the worker.  This module is the last durable boundary before MQTT publication:
the exact canonical envelope is appended, its head is advanced with
compare-and-swap, and an idempotency receipt is committed in one SQLite
transaction.  Home Assistant therefore never has to reconstruct state or read
the meteorological databases.

Only the active head pointer and a checksummed retention checkpoint are
mutable.  Snapshot envelopes and operation receipts are immutable while they
remain in the bounded, contiguous tail.  Prefix pruning is part of the same
SQLite transaction as the next compare-and-swap commit, so a crash exposes
either the complete old tail or the complete new tail.  The store deliberately
contains no provider payloads, rasters, observations, target coordinates or
credentials; the contract itself is bounded to 64 KiB.
"""
from __future__ import annotations

from contextlib import closing
from functools import lru_cache
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import time
from typing import Any, Callable, Mapping

from .tracking_core.operational_snapshot import (
    MAX_CONTROL_ENVELOPE_BYTES,
    decode_control_envelope,
    encode_control_envelope,
)


STORE_SCHEMA_VERSION = 2
STORE_ALGORITHM_VERSION = "worker-operational-snapshot-store-v2"
LEGACY_STORE_SCHEMA_VERSION = 1
LEGACY_STORE_ALGORITHM_VERSION = "worker-operational-snapshot-store-v1"
COMMIT_CONTRACT = "stv3-operational-snapshot-commit-v1"
RECEIPT_CONTRACT = "stv3-operational-snapshot-receipt-v1"
METHOD = "worker_operational_snapshot_store_v1"
DEFAULT_STORE_PATH = Path(
    os.getenv(
        "STV3_OPERATIONAL_SNAPSHOT_DB",
        "/share/storm_tracker_worker/operational_snapshots.sqlite3",
    )
)
MAX_DATABASE_BYTES = 128 * 1024 * 1024
MAX_SNAPSHOT_ROWS = 250_000
MAX_COMMIT_RECEIPTS = 500_000
MAX_RESULT_BYTES = 2 * 1024
DEFAULT_RETENTION_ROWS = 4_096

_COMMIT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,191}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STORED_COMMIT_ID_RE = re.compile(r"^commit-[0-9a-f]{64}$")
_AUTH_VALUE_RE = re.compile(
    r"\b(?:Bearer|Basic)\s+[A-Za-z0-9+/_.=-]{8,}", re.IGNORECASE
)
_JWT_VALUE_RE = re.compile(
    r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{12,}\."
    r"[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])"
)
_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?:access[_-]?token|refresh[_-]?token|api[_-]?key|client[_-]?secret|"
    r"authorization|password|passwd|secret|credential|private[_-]?key|"
    r"x-amz-signature|signature|sig|(?:^|[?&|;,])k)\s*=",
    re.IGNORECASE,
)
_RESULT_KEYS = frozenset({
    "committed_at",
    "envelope_bytes",
    "envelope_sha256",
    "method",
    "revision",
    "snapshot_sha256",
})


class OperationalSnapshotStoreError(RuntimeError):
    """Base class for durable snapshot-store failures."""


class SnapshotCommitConflict(OperationalSnapshotStoreError):
    """A commit ID was reused for different operation material."""


class SnapshotHeadConflict(OperationalSnapshotStoreError):
    """The expected revision is no longer the active head."""


class SnapshotCommitWindowExpired(OperationalSnapshotStoreError):
    """The requested idempotency receipt predates the retained tail."""


class SnapshotLegacyContractMigrationRequired(OperationalSnapshotStoreError):
    """A v1 store contains pre-expiry envelopes that need explicit conversion."""


class SnapshotStoreCapacityError(OperationalSnapshotStoreError):
    """A configured durable-store capacity limit was reached."""


class SnapshotStoreCorruption(OperationalSnapshotStoreError):
    """Persisted facts no longer match their canonical hashes or schema."""


def _canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError, UnicodeEncodeError) as err:
        raise OperationalSnapshotStoreError(
            "snapshot-storewaarde is niet canoniek JSON"
        ) from err


def _commit_id(value: object) -> str:
    if not isinstance(value, str) or _COMMIT_ID_RE.fullmatch(value) is None:
        raise OperationalSnapshotStoreError("commit_id is geen veilig begrensd ID")
    # The caller's idempotency key is transport material.  Persist only its
    # one-way identity so a token-shaped value can never enter the database.
    return f"commit-{hashlib.sha256(value.encode('ascii')).hexdigest()}"


def _revision(value: object, name: str, *, minimum: int) -> int:
    if type(value) is not int or not minimum <= value <= 1_000_000_000:
        raise OperationalSnapshotStoreError(f"{name} is geen geldige revision")
    return value


def _operation_digest(
    *,
    expected_revision: int,
    envelope_sha256: str,
) -> str:
    operation = _canonical_json({
        "contract": COMMIT_CONTRACT,
        "envelope_sha256": envelope_sha256,
        "expected_revision": expected_revision,
    })
    return hashlib.sha256(operation).hexdigest()


def _receipt_digest(
    *,
    commit_id: str,
    operation_sha256: str,
    revision: int,
    envelope_sha256: str,
    result_sha256: str,
    committed_at: float,
) -> str:
    receipt = _canonical_json({
        "commit_id": commit_id,
        "committed_at": committed_at,
        "contract": RECEIPT_CONTRACT,
        "envelope_sha256": envelope_sha256,
        "operation_sha256": operation_sha256,
        "result_sha256": result_sha256,
        "revision": revision,
    })
    return hashlib.sha256(receipt).hexdigest()


def _retention_digest(
    *,
    first_retained_revision: int,
    pruned_through_revision: int,
    retained_revision_count: int,
    head_revision: int,
    head_envelope_sha256: str,
    last_pruned_receipt_sha256: str | None,
    updated_at: float,
) -> str:
    state = _canonical_json({
        "contract": "stv3-operational-snapshot-retention-v1",
        "first_retained_revision": first_retained_revision,
        "head_envelope_sha256": head_envelope_sha256,
        "head_revision": head_revision,
        "last_pruned_receipt_sha256": last_pruned_receipt_sha256,
        "pruned_through_revision": pruned_through_revision,
        "retained_revision_count": retained_revision_count,
        "updated_at": updated_at,
    })
    return hashlib.sha256(state).hexdigest()


def _stored_timestamp(
    value: object,
    name: str,
    *,
    optional: bool = False,
) -> float | None:
    if value is None and optional:
        return None
    if type(value) not in {int, float}:
        raise SnapshotStoreCorruption(f"{name} is geen opgeslagen tijd")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise SnapshotStoreCorruption(f"{name} is geen opgeslagen tijd")
    return result


def _input_timestamp(value: object, name: str) -> float:
    if type(value) not in {int, float}:
        raise OperationalSnapshotStoreError(f"{name} is ongeldig")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise OperationalSnapshotStoreError(f"{name} is ongeldig")
    return result


def _reject_secret_material(value: Any, path: str = "$") -> None:
    """Reject credential-shaped text before any snapshot bytes are persisted."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            _reject_secret_material(child, f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            _reject_secret_material(child, f"{path}[{index}]")
        return
    if isinstance(value, str) and (
        _AUTH_VALUE_RE.search(value)
        or _JWT_VALUE_RE.search(value)
        or _SECRET_ASSIGNMENT_RE.search(value)
    ):
        raise OperationalSnapshotStoreError(
            f"{path} bevat mogelijk geheim transportmateriaal"
        )


def _normalized_schema_sql(value: str | None) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SnapshotStoreCorruption("snapshot-storeschema mist SQL")
    return " ".join(value.split()).lower()


class OperationalSnapshotStore:
    """Bounded canonical tail with one crash-durable CAS head."""

    _EXPECTED_COLUMNS = {
        "manifest": frozenset({"key", "value"}),
        "snapshot_envelopes": frozenset({
            "revision",
            "snapshot_sha256",
            "envelope_sha256",
            "envelope_bytes",
            "canonical_envelope",
            "product_valid_at",
            "received_at",
            "evaluated_at",
            "committed_at",
        }),
        "active_snapshot": frozenset({
            "singleton",
            "revision",
            "envelope_sha256",
            "activated_at",
        }),
        "snapshot_commit_receipts": frozenset({
            "commit_id",
            "operation_sha256",
            "revision",
            "envelope_sha256",
            "canonical_result",
            "result_sha256",
            "receipt_sha256",
            "committed_at",
        }),
        "snapshot_retention_state": frozenset({
            "singleton",
            "first_retained_revision",
            "pruned_through_revision",
            "retained_revision_count",
            "head_revision",
            "head_envelope_sha256",
            "last_pruned_receipt_sha256",
            "updated_at",
            "state_sha256",
        }),
    }

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
        max_database_bytes: int = MAX_DATABASE_BYTES,
        max_snapshot_rows: int = MAX_SNAPSHOT_ROWS,
        max_commit_receipts: int = MAX_COMMIT_RECEIPTS,
        retention_rows: int = DEFAULT_RETENTION_ROWS,
    ) -> None:
        self.path = Path(DEFAULT_STORE_PATH if path is None else path)
        self._clock = clock
        self._fault_injector = fault_injector
        for name, value, minimum in (
            ("max_database_bytes", max_database_bytes, 1024 * 1024),
            ("max_snapshot_rows", max_snapshot_rows, 1),
            ("max_commit_receipts", max_commit_receipts, 1),
            ("retention_rows", retention_rows, 2),
        ):
            if type(value) is not int or value < minimum:
                raise ValueError(f"{name} moet minstens {minimum} zijn")
        self._max_database_bytes = max_database_bytes
        self._max_snapshot_rows = max_snapshot_rows
        self._max_commit_receipts = max_commit_receipts
        self._retention_rows = retention_rows
        self._ready = False

    def _connect(self, *, configure: bool = True) -> sqlite3.Connection:
        db = sqlite3.connect(self.path, timeout=15.0, isolation_level=None)
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=15000")
            db.execute("PRAGMA temp_store=MEMORY")
            if configure:
                self._configure_connection(db)
            return db
        except BaseException:
            db.close()
            raise

    def _configure_connection(self, db: sqlite3.Connection) -> None:
        if int(db.execute("PRAGMA auto_vacuum").fetchone()[0]) != 0:
            raise SnapshotStoreCorruption(
                "snapshot-store vereist herbruikbare freelistpagina's zonder auto-vacuum"
            )
        journal_mode = str(
            db.execute("PRAGMA journal_mode=WAL").fetchone()[0]
        ).lower()
        if journal_mode != "wal":
            raise SnapshotStoreCorruption(
                f"snapshot-store kon WAL niet activeren: {journal_mode}"
            )
        db.execute("PRAGMA synchronous=FULL")
        db.execute("PRAGMA journal_size_limit=4194304")
        db.execute("PRAGMA wal_autocheckpoint=256")
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
        maximum_pages = self._max_database_bytes // page_size
        if page_count > maximum_pages:
            raise SnapshotStoreCapacityError(
                "snapshotdatabase is groter dan het ingestelde bytebudget"
            )
        applied_pages = int(
            db.execute(
                f"PRAGMA max_page_count={maximum_pages}"
            ).fetchone()[0]
        )
        if applied_pages != maximum_pages:
            raise SnapshotStoreCapacityError(
                "SQLite kon het snapshotdatabasebudget niet afdwingen"
            )

    @staticmethod
    def _create_legacy_schema(db: sqlite3.Connection) -> None:
        db.executescript(
            """
            CREATE TABLE IF NOT EXISTS manifest (
                key TEXT PRIMARY KEY CHECK(length(key) BETWEEN 1 AND 64),
                value TEXT NOT NULL CHECK(length(value) BETWEEN 1 AND 256)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS snapshot_envelopes (
                revision INTEGER PRIMARY KEY CHECK(revision >= 1),
                snapshot_sha256 TEXT NOT NULL
                    CHECK(length(snapshot_sha256) = 64
                          AND snapshot_sha256 NOT GLOB '*[^0-9a-f]*'),
                envelope_sha256 TEXT NOT NULL UNIQUE
                    CHECK(length(envelope_sha256) = 64
                          AND envelope_sha256 NOT GLOB '*[^0-9a-f]*'),
                envelope_bytes INTEGER NOT NULL
                    CHECK(envelope_bytes > 0 AND envelope_bytes <= 65536),
                canonical_envelope BLOB NOT NULL
                    CHECK(length(canonical_envelope) = envelope_bytes),
                product_valid_at REAL
                    CHECK(product_valid_at IS NULL OR product_valid_at >= 0),
                received_at REAL
                    CHECK(received_at IS NULL OR received_at >= 0),
                evaluated_at REAL NOT NULL CHECK(evaluated_at >= 0),
                committed_at REAL NOT NULL CHECK(committed_at >= 0),
                UNIQUE(revision, envelope_sha256)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS active_snapshot (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                revision INTEGER NOT NULL,
                envelope_sha256 TEXT NOT NULL,
                activated_at REAL NOT NULL CHECK(activated_at >= 0),
                FOREIGN KEY(revision, envelope_sha256)
                    REFERENCES snapshot_envelopes(revision, envelope_sha256)
            ) STRICT;
            CREATE TABLE IF NOT EXISTS snapshot_commit_receipts (
                commit_id TEXT PRIMARY KEY
                    CHECK(length(commit_id) = 71
                          AND substr(commit_id, 1, 7) = 'commit-'
                          AND substr(commit_id, 8) NOT GLOB '*[^0-9a-f]*'),
                operation_sha256 TEXT NOT NULL
                    CHECK(length(operation_sha256) = 64
                          AND operation_sha256 NOT GLOB '*[^0-9a-f]*'),
                revision INTEGER NOT NULL UNIQUE,
                envelope_sha256 TEXT NOT NULL UNIQUE,
                canonical_result BLOB NOT NULL
                    CHECK(length(canonical_result) BETWEEN 1 AND 2048),
                result_sha256 TEXT NOT NULL
                    CHECK(length(result_sha256) = 64
                          AND result_sha256 NOT GLOB '*[^0-9a-f]*'),
                receipt_sha256 TEXT NOT NULL
                    CHECK(length(receipt_sha256) = 64
                          AND receipt_sha256 NOT GLOB '*[^0-9a-f]*'),
                committed_at REAL NOT NULL CHECK(committed_at >= 0),
                FOREIGN KEY(revision, envelope_sha256)
                    REFERENCES snapshot_envelopes(revision, envelope_sha256)
            ) STRICT;
            """
        )

    @staticmethod
    def _create_retention_schema(db: sqlite3.Connection) -> None:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshot_retention_state (
                singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                first_retained_revision INTEGER NOT NULL
                    CHECK(first_retained_revision >= 1),
                pruned_through_revision INTEGER NOT NULL
                    CHECK(pruned_through_revision >= 0),
                retained_revision_count INTEGER NOT NULL
                    CHECK(retained_revision_count >= 1),
                head_revision INTEGER NOT NULL CHECK(head_revision >= 1),
                head_envelope_sha256 TEXT NOT NULL
                    CHECK(length(head_envelope_sha256) = 64
                          AND head_envelope_sha256 NOT GLOB '*[^0-9a-f]*'),
                last_pruned_receipt_sha256 TEXT
                    CHECK(last_pruned_receipt_sha256 IS NULL OR
                          (length(last_pruned_receipt_sha256) = 64 AND
                           last_pruned_receipt_sha256 NOT GLOB '*[^0-9a-f]*')),
                updated_at REAL NOT NULL CHECK(updated_at >= 0),
                state_sha256 TEXT NOT NULL
                    CHECK(length(state_sha256) = 64
                          AND state_sha256 NOT GLOB '*[^0-9a-f]*'),
                CHECK(first_retained_revision = pruned_through_revision + 1),
                CHECK(retained_revision_count =
                      head_revision - first_retained_revision + 1),
                CHECK((pruned_through_revision = 0 AND
                       last_pruned_receipt_sha256 IS NULL) OR
                      (pruned_through_revision > 0 AND
                       last_pruned_receipt_sha256 IS NOT NULL)),
                FOREIGN KEY(head_revision, head_envelope_sha256)
                    REFERENCES snapshot_envelopes(revision, envelope_sha256)
            ) STRICT;
            """
        )

    @classmethod
    def _create_schema(cls, db: sqlite3.Connection) -> None:
        cls._create_legacy_schema(db)
        cls._create_retention_schema(db)

    @classmethod
    @lru_cache(maxsize=1)
    def _expected_schema_objects(cls) -> tuple[tuple[str, str, str, str], ...]:
        """Build the exact persisted schema contract from trusted DDL."""
        with closing(sqlite3.connect(":memory:")) as expected:
            cls._create_schema(expected)
            return tuple(
                (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    _normalized_schema_sql(row[3]),
                )
                for row in expected.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' "
                    "ORDER BY type,name"
                )
            )

    @classmethod
    @lru_cache(maxsize=1)
    def _legacy_schema_objects(cls) -> tuple[tuple[str, str, str, str], ...]:
        with closing(sqlite3.connect(":memory:")) as expected:
            cls._create_legacy_schema(expected)
            return tuple(
                (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    _normalized_schema_sql(row[3]),
                )
                for row in expected.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' "
                    "ORDER BY type,name"
                )
            )

    def _prepare_schema(
        self,
        db: sqlite3.Connection,
        expected_manifest: Mapping[str, str],
    ) -> None:
        """Create only an empty/new schema; never pollute another database."""
        existing = tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                _normalized_schema_sql(row[3]),
            )
            for row in db.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' "
                "ORDER BY type,name"
            )
        )
        if not existing:
            self._create_schema(db)
            return
        if existing == self._legacy_schema_objects():
            legacy_manifest = dict(db.execute("SELECT key,value FROM manifest"))
            if legacy_manifest == {
                "algorithm_version": LEGACY_STORE_ALGORITHM_VERSION,
                "schema_version": str(LEGACY_STORE_SCHEMA_VERSION),
            }:
                self._migrate_legacy_store(db, expected_manifest)
                return
            if not legacy_manifest and not any(
                db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in (
                    "active_snapshot",
                    "snapshot_envelopes",
                    "snapshot_commit_receipts",
                )
            ):
                self._create_retention_schema(db)
                return
            raise SnapshotStoreCorruption(
                "legacy snapshot-storemanifest is onbekend of onvolledig"
            )
        trusted_objects = {
            (item[0], item[1]): item
            for item in self._expected_schema_objects()
        }
        if any(
            trusted_objects.get((item[0], item[1])) != item
            for item in existing
        ):
            raise SnapshotStoreCorruption(
                "bestaande databaseschema is geen snapshot-store"
            )
        table_names = {item[1] for item in existing if item[0] == "table"}
        if "manifest" in table_names:
            manifest = dict(db.execute("SELECT key,value FROM manifest"))
            if manifest:
                if manifest != dict(expected_manifest):
                    raise SnapshotStoreCorruption(
                        "snapshot-storemanifest is onbekend of onvolledig"
                    )
                if existing != self._expected_schema_objects():
                    raise SnapshotStoreCorruption(
                        "gemanifesteerde snapshot-store heeft partieel schema"
                    )
                return
        for table in sorted(table_names):
            if db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]:
                raise SnapshotStoreCorruption(
                    "partieel snapshot-storeschema bevat al gegevens"
                )
        self._create_schema(db)

    def _migrate_legacy_store(
        self,
        db: sqlite3.Connection,
        expected_manifest: Mapping[str, str],
    ) -> None:
        """Atomically migrate an exactly verified v1 store to a bounded v2 tail."""

        db.execute("BEGIN EXCLUSIVE")
        try:
            objects = tuple(
                (
                    str(row[0]),
                    str(row[1]),
                    str(row[2]),
                    _normalized_schema_sql(row[3]),
                )
                for row in db.execute(
                    "SELECT type,name,tbl_name,sql FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name"
                )
            )
            if objects != self._legacy_schema_objects():
                raise SnapshotStoreCorruption(
                    "legacy snapshot-storeschema wijzigde tijdens migratie"
                )
            if dict(db.execute("SELECT key,value FROM manifest")) != {
                "algorithm_version": LEGACY_STORE_ALGORITHM_VERSION,
                "schema_version": str(LEGACY_STORE_SCHEMA_VERSION),
            }:
                raise SnapshotStoreCorruption(
                    "legacy snapshot-storemanifest wijzigde tijdens migratie"
                )

            # The expiry field was added to the snapshot contract after the
            # original v1 store schema.  Rewriting those canonical envelopes
            # would also require atomic reconciliation of the separate
            # publication outbox.  Detect that exact legacy shape before the
            # current decoder labels it generic corruption, and leave v1
            # wholly untouched for an explicit conversion procedure.
            for legacy_row in db.execute(
                "SELECT canonical_envelope FROM snapshot_envelopes"
            ):
                try:
                    decoded = json.loads(bytes(legacy_row[0]).decode("utf-8"))
                except (TypeError, ValueError, UnicodeError):
                    continue
                snapshot = (
                    decoded.get("snapshot")
                    if isinstance(decoded, Mapping)
                    else None
                )
                if (
                    isinstance(snapshot, Mapping)
                    and snapshot.get("contract") == "operational_snapshot_v1"
                    and "valid_until" not in snapshot
                ):
                    raise SnapshotLegacyContractMigrationRequired(
                        "legacy snapshot mist valid_until; expliciete "
                        "expiryconversie en outboxreconciliatie zijn vereist"
                    )

            verified = self._verify_legacy_head(db)
            rows = db.execute(
                "SELECT * FROM snapshot_envelopes ORDER BY revision"
            ).fetchall()
            receipts = db.execute(
                "SELECT * FROM snapshot_commit_receipts ORDER BY revision"
            ).fetchall()
            if len(rows) != len(receipts):
                raise SnapshotStoreCorruption(
                    "legacy snapshot- en commitreceiptaantallen verschillen"
                )
            for expected_revision, (row, receipt) in enumerate(
                zip(rows, receipts, strict=True),
                start=1,
            ):
                if (
                    row["revision"] != expected_revision
                    or receipt["revision"] != expected_revision
                ):
                    raise SnapshotStoreCorruption(
                        "legacy snapshotgeschiedenis heeft een revisiongat"
                    )
                try:
                    self._decode_stored_envelope(row)
                except SnapshotStoreCorruption as error:
                    raw = row["canonical_envelope"]
                    try:
                        decoded = json.loads(bytes(raw).decode("utf-8"))
                    except (
                        TypeError,
                        ValueError,
                        UnicodeError,
                        json.JSONDecodeError,
                    ):
                        raise error
                    snapshot = (
                        decoded.get("snapshot")
                        if isinstance(decoded, Mapping)
                        else None
                    )
                    if (
                        isinstance(snapshot, Mapping)
                        and snapshot.get("contract") == "operational_snapshot_v1"
                        and "valid_until" not in snapshot
                    ):
                        raise SnapshotLegacyContractMigrationRequired(
                            "legacy snapshot mist valid_until; expliciete "
                            "expiryconversie en outboxreconciliatie zijn vereist"
                        ) from error
                    raise
                self._validate_receipt(receipt, row)

            last_pruned_receipt_sha: str | None = None
            first_retained = 1
            if verified is not None:
                head_row = verified[0]
                head_revision = int(head_row["revision"])
                first_retained = max(
                    1,
                    head_revision - self._retention_rows + 1,
                )
                if first_retained > 1:
                    anchor = db.execute(
                        "SELECT receipt_sha256 FROM snapshot_commit_receipts "
                        "WHERE revision=?",
                        (first_retained - 1,),
                    ).fetchone()
                    if (
                        anchor is None
                        or not isinstance(anchor[0], str)
                        or _SHA256_RE.fullmatch(anchor[0]) is None
                    ):
                        raise SnapshotStoreCorruption(
                            "legacy migratie mist prune-anchor"
                        )
                    last_pruned_receipt_sha = str(anchor[0])
                    db.execute(
                        "DELETE FROM snapshot_commit_receipts WHERE revision<?",
                        (first_retained,),
                    )
                    self._fault("migration_after_receipt_prune")
                    db.execute(
                        "DELETE FROM snapshot_envelopes WHERE revision<?",
                        (first_retained,),
                    )
                    self._fault("migration_after_envelope_prune")

            self._create_retention_schema(db)
            self._fault("migration_after_schema")
            if verified is not None:
                head_row = verified[0]
                self._write_retention_state(
                    db,
                    first_retained_revision=first_retained,
                    head_revision=int(head_row["revision"]),
                    head_envelope_sha256=str(head_row["envelope_sha256"]),
                    last_pruned_receipt_sha256=last_pruned_receipt_sha,
                    updated_at=float(head_row["committed_at"]),
                )
            db.executemany(
                "UPDATE manifest SET value=? WHERE key=?",
                (
                    (
                        str(expected_manifest["algorithm_version"]),
                        "algorithm_version",
                    ),
                    (
                        str(expected_manifest["schema_version"]),
                        "schema_version",
                    ),
                ),
            )
            self._fault("migration_before_commit")
            self._verify_schema(db)
            self._check_hot_manifest(db)
            self._verify_head(db)
            if db.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                raise SnapshotStoreCorruption(
                    "legacy migratie quick_check faalde"
                )
            if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise SnapshotStoreCorruption(
                    "legacy migratie foreign-keycheck faalde"
                )
            db.commit()
        except BaseException:
            if db.in_transaction:
                db.rollback()
            raise

    def initialize(self) -> None:
        if self._ready:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect(configure=False)) as db:
            expected = {
                "algorithm_version": STORE_ALGORITHM_VERSION,
                "schema_version": str(STORE_SCHEMA_VERSION),
            }
            self._prepare_schema(db, expected)
            self._configure_connection(db)
            db.execute("BEGIN IMMEDIATE")
            try:
                manifest = dict(db.execute("SELECT key,value FROM manifest"))
                if not manifest:
                    db.executemany(
                        "INSERT INTO manifest(key,value) VALUES(?,?)",
                        sorted(expected.items()),
                    )
                elif manifest != expected:
                    raise SnapshotStoreCorruption(
                        "snapshot-storemanifest is onbekend of onvolledig"
                    )
                self._verify_schema(db)
                if db.execute("PRAGMA quick_check(1)").fetchone()[0] != "ok":
                    raise SnapshotStoreCorruption("SQLite quick_check faalde")
                if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                    raise SnapshotStoreCorruption("SQLite foreign-keycheck faalde")
                self._verify_head(db)
                db.commit()
            except BaseException:
                db.rollback()
                raise
        self._ready = True

    def _verify_schema(self, db: sqlite3.Connection) -> None:
        objects = tuple(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                _normalized_schema_sql(row[3]),
            )
            for row in db.execute(
                "SELECT type,name,tbl_name,sql FROM sqlite_master "
                "WHERE name NOT LIKE 'sqlite_%' "
                "ORDER BY type,name"
            )
        )
        if objects != self._expected_schema_objects():
            raise SnapshotStoreCorruption(
                "snapshot-storeschema bevat onbekende of gewijzigde objecten"
            )
        for table, expected in self._EXPECTED_COLUMNS.items():
            columns = frozenset(
                row[1]
                for row in db.execute(f"PRAGMA table_info({table})")
            )
            if columns != expected:
                raise SnapshotStoreCorruption(
                    f"snapshot-storeschema voor {table} wijkt af"
                )

    def _check_hot_manifest(self, db: sqlite3.Connection) -> None:
        self._verify_schema(db)
        manifest = dict(db.execute("SELECT key,value FROM manifest"))
        if manifest != {
            "algorithm_version": STORE_ALGORITHM_VERSION,
            "schema_version": str(STORE_SCHEMA_VERSION),
        }:
            raise SnapshotStoreCorruption("snapshot-storemanifest wijzigde")

    def _database_bytes(self, db: sqlite3.Connection) -> int:
        page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        return page_count * page_size

    def _database_used_bytes(self, db: sqlite3.Connection) -> int:
        page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
        freelist_count = int(db.execute("PRAGMA freelist_count").fetchone()[0])
        page_size = int(db.execute("PRAGMA page_size").fetchone()[0])
        if not 0 <= freelist_count <= page_count:
            raise SnapshotStoreCorruption("SQLite freelisttelling is ongeldig")
        return (page_count - freelist_count) * page_size

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    @staticmethod
    def _retention_from_row(row: Mapping[str, Any]) -> dict[str, Any]:
        if row["singleton"] != 1:
            raise SnapshotStoreCorruption("retention singleton is ongeldig")
        integer_fields = (
            "first_retained_revision",
            "pruned_through_revision",
            "retained_revision_count",
            "head_revision",
        )
        if any(type(row[field]) is not int for field in integer_fields):
            raise SnapshotStoreCorruption("retention revisions zijn ongeldig")
        first = int(row["first_retained_revision"])
        pruned = int(row["pruned_through_revision"])
        count = int(row["retained_revision_count"])
        head_revision = int(row["head_revision"])
        head_sha = row["head_envelope_sha256"]
        anchor = row["last_pruned_receipt_sha256"]
        updated_at = _stored_timestamp(row["updated_at"], "retention.updated_at")
        state_sha = row["state_sha256"]
        if (
            first < 1
            or pruned < 0
            or count < 1
            or head_revision < 1
            or first != pruned + 1
            or count != head_revision - first + 1
            or not isinstance(head_sha, str)
            or _SHA256_RE.fullmatch(head_sha) is None
            or not isinstance(state_sha, str)
            or _SHA256_RE.fullmatch(state_sha) is None
            or (
                pruned == 0
                and anchor is not None
            )
            or (
                pruned > 0
                and (
                    not isinstance(anchor, str)
                    or _SHA256_RE.fullmatch(anchor) is None
                )
            )
        ):
            raise SnapshotStoreCorruption("retention checkpoint is ongeldig")
        expected_sha = _retention_digest(
            first_retained_revision=first,
            pruned_through_revision=pruned,
            retained_revision_count=count,
            head_revision=head_revision,
            head_envelope_sha256=head_sha,
            last_pruned_receipt_sha256=anchor,
            updated_at=float(updated_at),
        )
        if state_sha != expected_sha:
            raise SnapshotStoreCorruption("retention checkpointchecksum wijkt af")
        return {
            "first_retained_revision": first,
            "pruned_through_revision": pruned,
            "retained_revision_count": count,
            "head_revision": head_revision,
            "head_envelope_sha256": head_sha,
            "last_pruned_receipt_sha256": anchor,
            "updated_at": float(updated_at),
        }

    @classmethod
    def _load_retention_state(
        cls,
        db: sqlite3.Connection,
    ) -> dict[str, Any] | None:
        rows = db.execute("SELECT * FROM snapshot_retention_state").fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise SnapshotStoreCorruption(
                "snapshot-store heeft meerdere retention checkpoints"
            )
        return cls._retention_from_row(rows[0])

    @staticmethod
    def _write_retention_state(
        db: sqlite3.Connection,
        *,
        first_retained_revision: int,
        head_revision: int,
        head_envelope_sha256: str,
        last_pruned_receipt_sha256: str | None,
        updated_at: float,
    ) -> None:
        pruned_through = first_retained_revision - 1
        retained_count = head_revision - first_retained_revision + 1
        state_sha = _retention_digest(
            first_retained_revision=first_retained_revision,
            pruned_through_revision=pruned_through,
            retained_revision_count=retained_count,
            head_revision=head_revision,
            head_envelope_sha256=head_envelope_sha256,
            last_pruned_receipt_sha256=last_pruned_receipt_sha256,
            updated_at=updated_at,
        )
        db.execute(
            "INSERT INTO snapshot_retention_state("
            "singleton,first_retained_revision,pruned_through_revision,"
            "retained_revision_count,head_revision,head_envelope_sha256,"
            "last_pruned_receipt_sha256,updated_at,state_sha256) "
            "VALUES(1,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(singleton) DO UPDATE SET "
            "first_retained_revision=excluded.first_retained_revision,"
            "pruned_through_revision=excluded.pruned_through_revision,"
            "retained_revision_count=excluded.retained_revision_count,"
            "head_revision=excluded.head_revision,"
            "head_envelope_sha256=excluded.head_envelope_sha256,"
            "last_pruned_receipt_sha256=excluded.last_pruned_receipt_sha256,"
            "updated_at=excluded.updated_at,state_sha256=excluded.state_sha256",
            (
                first_retained_revision,
                pruned_through,
                retained_count,
                head_revision,
                head_envelope_sha256,
                last_pruned_receipt_sha256,
                updated_at,
                state_sha,
            ),
        )

    @staticmethod
    def _decode_stored_envelope(row: sqlite3.Row) -> dict:
        raw_canonical = row["canonical_envelope"]
        if not isinstance(raw_canonical, (bytes, bytearray, memoryview)):
            raise SnapshotStoreCorruption("opgeslagen envelope is geen BLOB")
        canonical = bytes(raw_canonical)
        envelope_bytes = row["envelope_bytes"]
        if (
            type(envelope_bytes) is not int
            or not 0 < envelope_bytes <= MAX_CONTROL_ENVELOPE_BYTES
            or len(canonical) != envelope_bytes
        ):
            raise SnapshotStoreCorruption("opgeslagen envelope-lengte klopt niet")
        envelope_sha256 = hashlib.sha256(canonical).hexdigest()
        stored_envelope_sha256 = row["envelope_sha256"]
        stored_snapshot_sha256 = row["snapshot_sha256"]
        if (
            not isinstance(stored_envelope_sha256, str)
            or _SHA256_RE.fullmatch(stored_envelope_sha256) is None
            or envelope_sha256 != stored_envelope_sha256
        ):
            raise SnapshotStoreCorruption("opgeslagen envelopehash klopt niet")
        if (
            not isinstance(stored_snapshot_sha256, str)
            or _SHA256_RE.fullmatch(stored_snapshot_sha256) is None
        ):
            raise SnapshotStoreCorruption("opgeslagen snapshot-hash is ongeldig")
        try:
            envelope = decode_control_envelope(canonical)
        except (TypeError, ValueError) as err:
            raise SnapshotStoreCorruption("opgeslagen envelope is ongeldig") from err
        try:
            _reject_secret_material(envelope)
        except OperationalSnapshotStoreError as err:
            raise SnapshotStoreCorruption(
                "opgeslagen envelope schendt het privacycontract"
            ) from err
        revision = row["revision"]
        if (
            type(revision) is not int
            or not 1 <= revision <= 1_000_000_000
            or envelope["revision"] != revision
            or envelope["snapshot_sha256"] != stored_snapshot_sha256
        ):
            raise SnapshotStoreCorruption("opgeslagen envelope-metadata klopt niet")
        snapshot = envelope["snapshot"]
        stored_product_valid_at = _stored_timestamp(
            row["product_valid_at"], "product_valid_at", optional=True
        )
        stored_received_at = _stored_timestamp(
            row["received_at"], "received_at", optional=True
        )
        stored_evaluated_at = _stored_timestamp(
            row["evaluated_at"], "evaluated_at"
        )
        expected_product_valid_at = snapshot["product_valid_at"]
        expected_received_at = snapshot["received_at"]
        if (
            stored_product_valid_at
            != (
                None
                if expected_product_valid_at is None
                else float(expected_product_valid_at)
            )
            or stored_received_at
            != (
                None if expected_received_at is None else float(expected_received_at)
            )
            or stored_evaluated_at != float(snapshot["evaluated_at"])
        ):
            raise SnapshotStoreCorruption(
                "opgeslagen product-, receipt- of evaluatietijd klopt niet"
            )
        _stored_timestamp(row["committed_at"], "committed_at")
        return envelope

    def _verify_legacy_head(
        self,
        db: sqlite3.Connection,
    ) -> tuple[sqlite3.Row, dict[str, Any], int, int] | None:
        heads = db.execute("SELECT * FROM active_snapshot").fetchall()
        if not heads:
            snapshot_count = int(
                db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0]
            )
            receipt_count = int(
                db.execute(
                    "SELECT count(*) FROM snapshot_commit_receipts"
                ).fetchone()[0]
            )
            if snapshot_count or receipt_count:
                raise SnapshotStoreCorruption(
                    "snapshotfeiten bestaan zonder actieve head"
                )
            return None
        if len(heads) != 1 or heads[0]["singleton"] != 1:
            raise SnapshotStoreCorruption("snapshot-store heeft meerdere actieve heads")
        head = heads[0]
        head_revision = head["revision"]
        if type(head_revision) is not int or not 1 <= head_revision <= 1_000_000_000:
            raise SnapshotStoreCorruption(
                "actieve snapshot-head heeft ongeldige revision"
            )
        maximum_revision = db.execute(
            "SELECT max(revision) FROM snapshot_envelopes"
        ).fetchone()[0]
        if maximum_revision != head_revision:
            raise SnapshotStoreCorruption(
                "actieve snapshot-head is stil teruggezet of niet de nieuwste"
            )
        snapshot_count = int(
            db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0]
        )
        receipt_count = int(
            db.execute(
                "SELECT count(*) FROM snapshot_commit_receipts"
            ).fetchone()[0]
        )
        if snapshot_count != head_revision or receipt_count != snapshot_count:
            raise SnapshotStoreCorruption(
                "snapshotgeschiedenis heeft een revision- of receiptgat"
            )
        row = db.execute(
            "SELECT * FROM snapshot_envelopes "
            "WHERE revision=? AND envelope_sha256=?",
            (head_revision, head["envelope_sha256"]),
        ).fetchone()
        if row is None:
            raise SnapshotStoreCorruption("actieve snapshot-head verwijst verkeerd")
        envelope = self._decode_stored_envelope(row)
        activated_at = _stored_timestamp(head["activated_at"], "activated_at")
        committed_at = _stored_timestamp(row["committed_at"], "committed_at")
        if activated_at != committed_at:
            raise SnapshotStoreCorruption("activeringstijd wijkt af van de commit")
        receipts = db.execute(
            "SELECT * FROM snapshot_commit_receipts WHERE revision=?",
            (head_revision,),
        ).fetchall()
        if len(receipts) != 1:
            raise SnapshotStoreCorruption(
                "actieve snapshot heeft niet exact een commitreceipt"
            )
        self._validate_receipt(receipts[0], row)
        return row, envelope, snapshot_count, receipt_count

    def _verify_head(
        self,
        db: sqlite3.Connection,
    ) -> tuple[sqlite3.Row, dict[str, Any], int, int] | None:
        heads = db.execute("SELECT * FROM active_snapshot").fetchall()
        retention = self._load_retention_state(db)
        if not heads:
            snapshot_count = int(
                db.execute("SELECT count(*) FROM snapshot_envelopes").fetchone()[0]
            )
            receipt_count = int(
                db.execute(
                    "SELECT count(*) FROM snapshot_commit_receipts"
                ).fetchone()[0]
            )
            if snapshot_count or receipt_count or retention is not None:
                raise SnapshotStoreCorruption(
                    "snapshotfeiten bestaan zonder actieve head"
                )
            return None
        if len(heads) != 1 or heads[0]["singleton"] != 1:
            raise SnapshotStoreCorruption("snapshot-store heeft meerdere actieve heads")
        if retention is None:
            raise SnapshotStoreCorruption("snapshot-store mist retention checkpoint")
        head = heads[0]
        head_revision = head["revision"]
        if type(head_revision) is not int or not 1 <= head_revision <= 1_000_000_000:
            raise SnapshotStoreCorruption(
                "actieve snapshot-head heeft ongeldige revision"
            )
        maximum_revision = db.execute(
            "SELECT max(revision) FROM snapshot_envelopes"
        ).fetchone()[0]
        if maximum_revision != head_revision:
            raise SnapshotStoreCorruption(
                "actieve snapshot-head is stil teruggezet of niet de nieuwste"
            )
        snapshot_stats = db.execute(
            "SELECT min(revision),max(revision),count(*) FROM snapshot_envelopes"
        ).fetchone()
        receipt_stats = db.execute(
            "SELECT min(revision),max(revision),count(*) "
            "FROM snapshot_commit_receipts"
        ).fetchone()
        first = retention["first_retained_revision"]
        expected_count = retention["retained_revision_count"]
        snapshot_count = int(snapshot_stats[2])
        receipt_count = int(receipt_stats[2])
        if (
            snapshot_count != expected_count
            or snapshot_stats[0] != first
            or snapshot_stats[1] != head_revision
            or snapshot_count != head_revision - first + 1
        ):
            raise SnapshotStoreCorruption(
                "snapshotgeschiedenis heeft een revisiongat"
            )
        if (
            receipt_count != snapshot_count
            or receipt_stats[0] != first
            or receipt_stats[1] != head_revision
        ):
            raise SnapshotStoreCorruption(
                "snapshotgeschiedenis heeft een receiptgat"
            )
        if (
            retention["head_revision"] != head_revision
            or retention["head_envelope_sha256"] != head["envelope_sha256"]
        ):
            raise SnapshotStoreCorruption(
                "retention checkpoint wijkt af van actieve head"
            )
        row = db.execute(
            "SELECT * FROM snapshot_envelopes "
            "WHERE revision=? AND envelope_sha256=?",
            (head_revision, head["envelope_sha256"]),
        ).fetchone()
        if row is None:
            raise SnapshotStoreCorruption("actieve snapshot-head verwijst verkeerd")
        envelope = self._decode_stored_envelope(row)
        activated_at = _stored_timestamp(head["activated_at"], "activated_at")
        committed_at = _stored_timestamp(row["committed_at"], "committed_at")
        if activated_at != committed_at:
            raise SnapshotStoreCorruption("activeringstijd wijkt af van de commit")
        if retention["updated_at"] != committed_at:
            raise SnapshotStoreCorruption(
                "retention checkpointtijd wijkt af van actieve commit"
            )
        receipts = db.execute(
            "SELECT * FROM snapshot_commit_receipts WHERE revision=?",
            (head_revision,),
        ).fetchall()
        if len(receipts) != 1:
            raise SnapshotStoreCorruption(
                "actieve snapshot heeft niet exact een commitreceipt"
            )
        self._validate_receipt(receipts[0], row)
        return row, envelope, snapshot_count, receipt_count

    @staticmethod
    def _decode_result(
        raw: bytes,
        *,
        expected_revision: int,
        expected_snapshot_sha256: str,
        expected_envelope_sha256: str,
        expected_envelope_bytes: int,
        expected_committed_at: float,
    ) -> dict:
        if not raw or len(raw) > MAX_RESULT_BYTES:
            raise SnapshotStoreCorruption("commitreceipt-resultaat is te groot")
        try:
            result = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as err:
            raise SnapshotStoreCorruption(
                "commitreceipt bevat ongeldig resultaat"
            ) from err
        try:
            canonical = _canonical_json(result)
        except OperationalSnapshotStoreError as err:
            raise SnapshotStoreCorruption("commitreceipt is niet canoniek") from err
        if canonical != raw:
            raise SnapshotStoreCorruption("commitreceipt is niet canoniek")
        if (
            type(result) is not dict
            or set(result) != _RESULT_KEYS
            or result.get("method") != METHOD
            or type(result.get("revision")) is not int
            or result.get("revision") != expected_revision
            or result.get("snapshot_sha256") != expected_snapshot_sha256
            or result.get("envelope_sha256") != expected_envelope_sha256
            or type(result.get("envelope_bytes")) is not int
            or result.get("envelope_bytes") != expected_envelope_bytes
            or type(result.get("committed_at")) not in {int, float}
            or not math.isfinite(float(result["committed_at"]))
            or float(result["committed_at"]) != expected_committed_at
        ):
            raise SnapshotStoreCorruption("commitreceipt-resultaat klopt niet")
        return result

    @classmethod
    def _validate_receipt(
        cls,
        receipt: sqlite3.Row,
        snapshot_row: sqlite3.Row,
    ) -> dict[str, Any]:
        commit_id = receipt["commit_id"]
        operation_sha256 = receipt["operation_sha256"]
        result_sha256 = receipt["result_sha256"]
        receipt_sha256 = receipt["receipt_sha256"]
        revision = receipt["revision"]
        if (
            not isinstance(commit_id, str)
            or _STORED_COMMIT_ID_RE.fullmatch(commit_id) is None
            or not isinstance(operation_sha256, str)
            or _SHA256_RE.fullmatch(operation_sha256) is None
            or not isinstance(result_sha256, str)
            or _SHA256_RE.fullmatch(result_sha256) is None
            or not isinstance(receipt_sha256, str)
            or _SHA256_RE.fullmatch(receipt_sha256) is None
            or type(revision) is not int
        ):
            raise SnapshotStoreCorruption("commitreceipt-identiteit is ongeldig")
        if (
            revision != snapshot_row["revision"]
            or receipt["envelope_sha256"] != snapshot_row["envelope_sha256"]
        ):
            raise SnapshotStoreCorruption("commitreceipt verwijst naar andere snapshot")
        expected_operation_sha256 = _operation_digest(
            expected_revision=revision - 1,
            envelope_sha256=str(snapshot_row["envelope_sha256"]),
        )
        if operation_sha256 != expected_operation_sha256:
            raise SnapshotStoreCorruption("commitreceipt-operationhash klopt niet")
        committed_at = _stored_timestamp(
            receipt["committed_at"], "commitreceipt.committed_at"
        )
        snapshot_committed_at = _stored_timestamp(
            snapshot_row["committed_at"], "snapshot.committed_at"
        )
        if committed_at != snapshot_committed_at:
            raise SnapshotStoreCorruption("commitreceipt-tijd wijkt af")
        raw_result = receipt["canonical_result"]
        if not isinstance(raw_result, (bytes, bytearray, memoryview)):
            raise SnapshotStoreCorruption("commitreceipt-resultaat is geen BLOB")
        canonical_result = bytes(raw_result)
        if hashlib.sha256(canonical_result).hexdigest() != result_sha256:
            raise SnapshotStoreCorruption("commitreceipt-resultaathash klopt niet")
        expected_receipt_sha256 = _receipt_digest(
            commit_id=commit_id,
            operation_sha256=operation_sha256,
            revision=revision,
            envelope_sha256=str(snapshot_row["envelope_sha256"]),
            result_sha256=result_sha256,
            committed_at=float(committed_at),
        )
        if receipt_sha256 != expected_receipt_sha256:
            raise SnapshotStoreCorruption("commitreceipt-hash klopt niet")
        return cls._decode_result(
            canonical_result,
            expected_revision=revision,
            expected_snapshot_sha256=str(snapshot_row["snapshot_sha256"]),
            expected_envelope_sha256=str(snapshot_row["envelope_sha256"]),
            expected_envelope_bytes=int(snapshot_row["envelope_bytes"]),
            expected_committed_at=float(snapshot_committed_at),
        )

    def commit(
        self,
        commit_id: str,
        *,
        expected_revision: int,
        snapshot: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Atomically append and activate exactly one next snapshot revision."""
        self.initialize()
        operation_id = _commit_id(commit_id)
        expected = _revision(expected_revision, "expected_revision", minimum=0)
        envelope_bytes = encode_control_envelope(snapshot)
        envelope = decode_control_envelope(envelope_bytes)
        _reject_secret_material(envelope)
        revision = int(envelope["revision"])
        if revision != expected + 1:
            raise SnapshotHeadConflict(
                "snapshot.revision moet exact expected_revision + 1 zijn"
            )
        envelope_sha256 = hashlib.sha256(envelope_bytes).hexdigest()
        operation_sha256 = _operation_digest(
            expected_revision=expected,
            envelope_sha256=envelope_sha256,
        )
        committed_at = _input_timestamp(self._clock(), "storeklok")

        with closing(self._connect()) as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                self._check_hot_manifest(db)
                verified_head = self._verify_head(db)
                head = None if verified_head is None else verified_head[0]
                current_revision = 0 if head is None else int(head["revision"])
                snapshot_rows = 0 if verified_head is None else verified_head[2]
                receipt_rows = 0 if verified_head is None else verified_head[3]
                retention = self._load_retention_state(db)
                receipt = db.execute(
                    "SELECT * FROM snapshot_commit_receipts WHERE commit_id=?",
                    (operation_id,),
                ).fetchone()
                if receipt is not None:
                    receipt_snapshot = db.execute(
                        "SELECT * FROM snapshot_envelopes "
                        "WHERE revision=? AND envelope_sha256=?",
                        (receipt["revision"], receipt["envelope_sha256"]),
                    ).fetchone()
                    if receipt_snapshot is None:
                        raise SnapshotStoreCorruption(
                            "commitreceipt verwijst naar ontbrekende snapshot"
                        )
                    result = self._validate_receipt(receipt, receipt_snapshot)
                    if receipt["operation_sha256"] != operation_sha256:
                        raise SnapshotCommitConflict(
                            "commit_id werd voor andere snapshotdata hergebruikt"
                        )
                    db.commit()
                    return result

                if (
                    retention is not None
                    and expected < retention["first_retained_revision"] - 1
                ):
                    raise SnapshotCommitWindowExpired(
                        "commitreceipt valt buiten het bewaarde idempotentievenster"
                    )
                if current_revision != expected:
                    raise SnapshotHeadConflict(
                        f"verwachte head {expected}, actuele head {current_revision}"
                    )
                old_floor = (
                    1
                    if retention is None
                    else retention["first_retained_revision"]
                )
                new_floor = max(
                    old_floor,
                    revision - self._retention_rows + 1,
                )
                can_prune_prefix = new_floor > old_floor
                if (
                    snapshot_rows >= self._max_snapshot_rows
                    and not can_prune_prefix
                ):
                    raise SnapshotStoreCapacityError("snapshotrijlimiet is bereikt")
                if (
                    receipt_rows >= self._max_commit_receipts
                    and not can_prune_prefix
                ):
                    raise SnapshotStoreCapacityError("commitreceiptlimiet is bereikt")
                last_pruned_receipt_sha = (
                    None
                    if retention is None
                    else retention["last_pruned_receipt_sha256"]
                )
                if new_floor > old_floor:
                    anchor = db.execute(
                        "SELECT receipt_sha256 FROM snapshot_commit_receipts "
                        "WHERE revision=?",
                        (new_floor - 1,),
                    ).fetchone()
                    if (
                        anchor is None
                        or not isinstance(anchor[0], str)
                        or _SHA256_RE.fullmatch(anchor[0]) is None
                    ):
                        raise SnapshotStoreCorruption(
                            "retentiepruning mist exacte receipt-anchor"
                        )
                    last_pruned_receipt_sha = str(anchor[0])
                    db.execute(
                        "DELETE FROM snapshot_commit_receipts WHERE revision<?",
                        (new_floor,),
                    )
                    self._fault("after_retention_receipt_prune")
                    db.execute(
                        "DELETE FROM snapshot_envelopes WHERE revision<?",
                        (new_floor,),
                    )
                    self._fault("after_retention_envelope_prune")

                if self._database_used_bytes(db) >= self._max_database_bytes:
                    raise SnapshotStoreCapacityError(
                        "snapshotdatabasebudget is bereikt"
                    )

                inner = envelope["snapshot"]
                product_valid_at = inner["product_valid_at"]
                received_at = inner["received_at"]
                db.execute(
                    "INSERT INTO snapshot_envelopes("
                    "revision,snapshot_sha256,envelope_sha256,envelope_bytes,"
                    "canonical_envelope,product_valid_at,received_at,evaluated_at,"
                    "committed_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        revision,
                        envelope["snapshot_sha256"],
                        envelope_sha256,
                        len(envelope_bytes),
                        envelope_bytes,
                        (
                            None
                            if product_valid_at is None
                            else float(product_valid_at)
                        ),
                        None if received_at is None else float(received_at),
                        float(inner["evaluated_at"]),
                        committed_at,
                    ),
                )
                self._fault("after_snapshot_insert")
                db.execute(
                    "INSERT INTO active_snapshot("
                    "singleton,revision,envelope_sha256,activated_at) VALUES(1,?,?,?) "
                    "ON CONFLICT(singleton) DO UPDATE SET "
                    "revision=excluded.revision,"
                    "envelope_sha256=excluded.envelope_sha256,"
                    "activated_at=excluded.activated_at",
                    (revision, envelope_sha256, committed_at),
                )
                self._fault("after_head_update")
                result = {
                    "committed_at": committed_at,
                    "envelope_bytes": len(envelope_bytes),
                    "envelope_sha256": envelope_sha256,
                    "method": METHOD,
                    "revision": revision,
                    "snapshot_sha256": envelope["snapshot_sha256"],
                }
                canonical_result = _canonical_json(result)
                if len(canonical_result) > MAX_RESULT_BYTES:
                    raise OperationalSnapshotStoreError(
                        "commitresultaat overschrijdt bytebudget"
                    )
                result_sha256 = hashlib.sha256(canonical_result).hexdigest()
                receipt_sha256 = _receipt_digest(
                    commit_id=operation_id,
                    operation_sha256=operation_sha256,
                    revision=revision,
                    envelope_sha256=envelope_sha256,
                    result_sha256=result_sha256,
                    committed_at=committed_at,
                )
                db.execute(
                    "INSERT INTO snapshot_commit_receipts("
                    "commit_id,operation_sha256,revision,envelope_sha256,"
                    "canonical_result,result_sha256,receipt_sha256,committed_at) "
                    "VALUES(?,?,?,?,?,?,?,?)",
                    (
                        operation_id,
                        operation_sha256,
                        revision,
                        envelope_sha256,
                        canonical_result,
                        result_sha256,
                        receipt_sha256,
                        committed_at,
                    ),
                )
                self._fault("after_receipt_insert")
                self._write_retention_state(
                    db,
                    first_retained_revision=new_floor,
                    head_revision=revision,
                    head_envelope_sha256=envelope_sha256,
                    last_pruned_receipt_sha256=last_pruned_receipt_sha,
                    updated_at=committed_at,
                )
                self._fault("after_retention_update")
                self._fault("before_commit")
                db.commit()
            except sqlite3.OperationalError as err:
                db.rollback()
                if (
                    getattr(err, "sqlite_errorcode", None) == sqlite3.SQLITE_FULL
                    or "database or disk is full" in str(err).lower()
                ):
                    raise SnapshotStoreCapacityError(
                        "SQLite snapshotdatabasebudget is bereikt"
                    ) from err
                raise
            except BaseException:
                db.rollback()
                raise
        self._fault("after_commit")
        return result

    def head(self) -> dict[str, Any] | None:
        """Return a verified bounded head, or ``None`` before first commit."""
        self.initialize()
        with closing(self._connect()) as db:
            self._check_hot_manifest(db)
            verified_head = self._verify_head(db)
            if verified_head is None:
                return None
            row, envelope, _snapshot_count, _receipt_count = verified_head
            return {
                "canonical_envelope": bytes(row["canonical_envelope"]),
                "committed_at": float(row["committed_at"]),
                "envelope_bytes": int(row["envelope_bytes"]),
                "envelope_sha256": str(row["envelope_sha256"]),
                "revision": int(row["revision"]),
                "snapshot_sha256": str(row["snapshot_sha256"]),
                "snapshot": envelope["snapshot"],
            }

    def retention_status(self) -> dict[str, Any]:
        """Return bounded storage diagnostics without snapshot content."""

        self.initialize()
        with closing(self._connect()) as db:
            self._check_hot_manifest(db)
            verified = self._verify_head(db)
            retention = self._load_retention_state(db)
            page_count = int(db.execute("PRAGMA page_count").fetchone()[0])
            freelist_count = int(
                db.execute("PRAGMA freelist_count").fetchone()[0]
            )
            return {
                "first_retained_revision": (
                    None
                    if retention is None
                    else retention["first_retained_revision"]
                ),
                "freelist_pages": freelist_count,
                "head_revision": 0 if verified is None else int(verified[0]["revision"]),
                "page_count": page_count,
                "retained_revisions": 0 if verified is None else verified[2],
                "retention_rows": self._retention_rows,
                "used_database_bytes": self._database_used_bytes(db),
            }

    def audit(self) -> dict[str, Any]:
        """Explicit full integrity audit; never run on the publication hot path."""
        self.initialize()
        with closing(self._connect()) as db:
            self._check_hot_manifest(db)
            self._verify_schema(db)
            if db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise SnapshotStoreCorruption("SQLite quick_check faalde")
            if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise SnapshotStoreCorruption("SQLite foreign-keycheck faalde")
            verified = self._verify_head(db)
            retention = self._load_retention_state(db)
            first_retained = (
                1
                if retention is None
                else retention["first_retained_revision"]
            )
            rows = db.execute(
                "SELECT * FROM snapshot_envelopes ORDER BY revision"
            ).fetchall()
            for expected_revision, row in enumerate(
                rows,
                start=first_retained,
            ):
                if int(row["revision"]) != expected_revision:
                    raise SnapshotStoreCorruption("snapshotrevisionreeks bevat een gat")
                self._decode_stored_envelope(row)
            receipts = db.execute(
                "SELECT * FROM snapshot_commit_receipts"
            ).fetchall()
            if len(receipts) != len(rows):
                raise SnapshotStoreCorruption(
                    "snapshot- en commitreceiptaantallen verschillen"
                )
            rows_by_revision = {int(row["revision"]): row for row in rows}
            receipted_revisions: set[int] = set()
            for receipt in receipts:
                revision = receipt["revision"]
                if type(revision) is not int or revision in receipted_revisions:
                    raise SnapshotStoreCorruption(
                        "commitreceipts bevatten een dubbele revision"
                    )
                snapshot_row = rows_by_revision.get(revision)
                if snapshot_row is None:
                    raise SnapshotStoreCorruption(
                        "commitreceipt verwijst naar ontbrekende revision"
                    )
                self._validate_receipt(receipt, snapshot_row)
                receipted_revisions.add(revision)
            if (verified is None) != (not rows):
                raise SnapshotStoreCorruption(
                    "snapshotaudit en actieve head spreken elkaar tegen"
                )
            self._verify_head(db)
            return {
                "commit_receipts": len(receipts),
                "database_bytes": self._database_bytes(db),
                "method": METHOD,
                "snapshots": len(rows),
                "status": "ok",
            }


def operational_snapshot_commit(job) -> dict[str, Any]:
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or set(payload) != {
        "commit_id", "expected_revision", "snapshot"
    }:
        raise OperationalSnapshotStoreError(
            "operational_snapshot_commit vereist exact commit_id, "
            "expected_revision en snapshot"
        )
    return OperationalSnapshotStore().commit(
        payload["commit_id"],
        expected_revision=payload["expected_revision"],
        snapshot=payload["snapshot"],
    )


def operational_snapshot_head(job) -> dict[str, Any]:
    payload = getattr(job, "payload", None)
    if type(payload) is not dict or payload:
        raise OperationalSnapshotStoreError(
            "operational_snapshot_head vereist exact een leeg object"
        )
    head = OperationalSnapshotStore().head()
    if head is None:
        return {"available": False, "method": METHOD}
    return {
        "available": True,
        "canonical_envelope": head["canonical_envelope"].decode("utf-8"),
        "committed_at": head["committed_at"],
        "envelope_bytes": head["envelope_bytes"],
        "envelope_sha256": head["envelope_sha256"],
        "method": METHOD,
        "revision": head["revision"],
        "snapshot_sha256": head["snapshot_sha256"],
    }


__all__ = [
    "COMMIT_CONTRACT",
    "DEFAULT_RETENTION_ROWS",
    "DEFAULT_STORE_PATH",
    "MAX_DATABASE_BYTES",
    "METHOD",
    "OperationalSnapshotStore",
    "OperationalSnapshotStoreError",
    "SnapshotCommitConflict",
    "SnapshotCommitWindowExpired",
    "SnapshotLegacyContractMigrationRequired",
    "SnapshotHeadConflict",
    "SnapshotStoreCapacityError",
    "SnapshotStoreCorruption",
    "operational_snapshot_commit",
    "operational_snapshot_head",
]
