"""Durable global raw-capacity reservation ledger candidate.

This append-only ledger is used by the deliberately unwired acquisition shadow
candidate.  It accounts reservations before a writer touches provider storage
and keeps unresolved reservations charged after crashes.  A separate closed
reconciler may convert exact durable captures to commits; this ledger itself
never deletes, releases, expires or invents capacity.
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
import stat
import threading
import time
from typing import Any, Callable, Literal


LEDGER_CONTRACT = "stv3-worker-global-raw-capacity-ledger-v1"
PROFILE_SCHEMA = "stv3-worker-global-raw-capacity-profile-v1"
RESERVATION_SCHEMA = "stv3-worker-raw-capacity-reservation-v1"
COMMIT_SCHEMA = "stv3-worker-raw-capacity-commit-v1"
SCHEMA_VERSION = 1
ALGORITHM_VERSION = "global-raw-capacity-ledger-shadow-v1"
ADMISSION_STATUS = "candidate_only_unadmitted"
JOURNAL_MODE = "delete"
MAIN_DATABASE_ENVELOPE_DIVISOR = 4
DIRECTORY_FSYNC_POLICY = "posix_required_windows_unavailable"
DIRECTORY_FSYNC_SUPPORTED = os.name == "posix"
MIN_CATALOG_BYTES = 1024 * 1024
MAX_PROVIDERS = 64
MAX_ID_CHARS = 256
MAX_RESERVATIONS_HARD = 2_000_000
_SQLITE_MIN_PAGE_SIZE_BYTES = 512
_SQLITE_MAX_PAGE_SIZE_BYTES = 64 * 1024
_SQLITE_BTREE_GROWTH_PAGES_PER_RESERVATION = 24
_LEDGER_RESERVATION_FIXED_RESERVE_BYTES = 4 * 1024

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,255}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STATUSES = frozenset({"RESERVED", "COMMITTED"})


def _validated_sqlite_page_size(value: object) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not _SQLITE_MIN_PAGE_SIZE_BYTES
        <= value
        <= _SQLITE_MAX_PAGE_SIZE_BYTES
        or value & (value - 1)
    ):
        raise ValueError("invalid SQLite page size")
    return value


def capacity_ledger_catalog_main_database_max_bytes(
    *,
    max_catalog_bytes: int,
    page_size_bytes: int,
) -> int:
    """Return the exact writable SQLite-main envelope for the ledger."""

    _positive_integer(max_catalog_bytes, "max_catalog_bytes")
    page_size = _validated_sqlite_page_size(page_size_bytes)
    maximum_pages = max(
        1,
        max_catalog_bytes
        // MAIN_DATABASE_ENVELOPE_DIVISOR
        // page_size,
    )
    return maximum_pages * page_size


def capacity_ledger_catalog_cycle_reserve_bytes(
    *,
    reservations: int,
    page_size_bytes: int,
) -> int:
    """Conservatively bound main-DB growth for one reservation cycle."""

    if (
        isinstance(reservations, bool)
        or not isinstance(reservations, int)
        or reservations < 0
    ):
        raise ValueError("reservations must be a bounded integer")
    page_size = _validated_sqlite_page_size(page_size_bytes)
    per_reservation = (
        _LEDGER_RESERVATION_FIXED_RESERVE_BYTES
        + _SQLITE_BTREE_GROWTH_PAGES_PER_RESERVATION * page_size
    )
    result = reservations * per_reservation
    if result > (1 << 63) - 1:
        raise ValueError("ledger catalog cycle reserve is too large")
    return max(page_size, result)


class RawCapacityLedgerError(RuntimeError):
    """Base class for a fail-closed ledger operation."""


class RawCapacityLedgerConflict(RawCapacityLedgerError):
    """A reservation/job identity was reused with different content."""


class RawCapacityLedgerCapacityError(RawCapacityLedgerError):
    """A global, provider, row or catalog capacity would be exceeded."""


class RawCapacityLedgerIntegrityError(RawCapacityLedgerError):
    """The catalog, schema, profile or durable invariants are invalid."""


class RawCapacityLedgerBusyError(RawCapacityLedgerError):
    """Another process held the bounded SQLite writer lock too long."""


@dataclass(frozen=True, slots=True)
class ProviderCapacityLimit:
    provider: str
    max_bytes: int
    max_entries: int

    def __post_init__(self) -> None:
        _identifier(self.provider, "provider")
        _positive_integer(self.max_bytes, "provider.max_bytes")
        _positive_integer(self.max_entries, "provider.max_entries")

    def to_mapping(self) -> dict[str, object]:
        return {
            "provider": self.provider,
            "max_bytes": self.max_bytes,
            "max_entries": self.max_entries,
        }


@dataclass(frozen=True, slots=True)
class RawCapacityProfile:
    global_max_bytes: int
    global_max_entries: int
    providers: tuple[ProviderCapacityLimit, ...]
    max_reservations: int = 1_000_000
    max_catalog_bytes: int = 256 * 1024 * 1024
    sqlite_busy_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        _positive_integer(self.global_max_bytes, "global_max_bytes")
        _positive_integer(self.global_max_entries, "global_max_entries")
        _positive_integer(self.max_reservations, "max_reservations")
        _positive_integer(self.max_catalog_bytes, "max_catalog_bytes")
        if self.max_reservations > MAX_RESERVATIONS_HARD:
            raise ValueError("max_reservations exceeds the hard bound")
        if self.max_catalog_bytes < MIN_CATALOG_BYTES:
            raise ValueError("max_catalog_bytes must be at least 1 MiB")
        if not isinstance(self.providers, tuple):
            raise TypeError("providers must be an immutable tuple")
        if not 1 <= len(self.providers) <= MAX_PROVIDERS:
            raise ValueError("providers count is outside the hard bound")
        if any(not isinstance(item, ProviderCapacityLimit) for item in self.providers):
            raise TypeError("providers must contain ProviderCapacityLimit values")
        names = [item.provider for item in self.providers]
        if names != sorted(set(names)):
            raise ValueError("providers must be unique and sorted")
        for item in self.providers:
            if item.max_bytes > self.global_max_bytes:
                raise ValueError("provider byte cap exceeds global byte cap")
            if item.max_entries > self.global_max_entries:
                raise ValueError("provider entry cap exceeds global entry cap")
        timeout = self.sqlite_busy_timeout_seconds
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or not 0.001 <= float(timeout) <= 30.0
        ):
            raise ValueError("sqlite_busy_timeout_seconds is invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            "schema": PROFILE_SCHEMA,
            "global_max_bytes": self.global_max_bytes,
            "global_max_entries": self.global_max_entries,
            "providers": [item.to_mapping() for item in self.providers],
            "max_reservations": self.max_reservations,
            "max_catalog_bytes": self.max_catalog_bytes,
            "sqlite_busy_timeout_seconds": float(
                self.sqlite_busy_timeout_seconds
            ),
            "journal_mode": JOURNAL_MODE,
            "main_database_envelope_divisor": MAIN_DATABASE_ENVELOPE_DIVISOR,
            "directory_fsync_policy": DIRECTORY_FSYNC_POLICY,
            "automatic_reclaim": False,
            "operational_admission_eligible": False,
        }


@dataclass(frozen=True, slots=True)
class CapacityReservationReceipt:
    reservation_id: str
    provider: str
    job_identity: str
    requested_bytes: int
    requested_entries: int
    request_sha256: str
    reservation_sha256: str
    status: Literal["RESERVED", "COMMITTED"]
    created_at: float
    actual_bytes: int | None
    actual_entries: int | None
    content_sha256: str | None
    receipt_sha256: str | None
    capture_sha256: str | None
    commit_sha256: str | None
    committed_at: float | None

    @property
    def operational_admission_eligible(self) -> bool:
        return False

    def to_mapping(self) -> dict[str, object]:
        return {
            "contract": LEDGER_CONTRACT,
            "admission_status": ADMISSION_STATUS,
            "operational_admission_eligible": False,
            "reservation_id": self.reservation_id,
            "provider": self.provider,
            "job_identity": self.job_identity,
            "requested_bytes": self.requested_bytes,
            "requested_entries": self.requested_entries,
            "request_sha256": self.request_sha256,
            "reservation_sha256": self.reservation_sha256,
            "status": self.status,
            "created_at": self.created_at,
            "actual_bytes": self.actual_bytes,
            "actual_entries": self.actual_entries,
            "content_sha256": self.content_sha256,
            "receipt_sha256": self.receipt_sha256,
            "capture_sha256": self.capture_sha256,
            "commit_sha256": self.commit_sha256,
            "committed_at": self.committed_at,
        }


def _positive_integer(value: object, name: str, *, allow_zero: bool = False) -> int:
    minimum = 0 if allow_zero else 1
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= 2**63 - 1
    ):
        raise ValueError(f"{name} must be a bounded integer")
    return value


def _identifier(value: object, name: str) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or _IDENTIFIER_RE.fullmatch(value) is None
    ):
        raise ValueError(f"{name} is not a bounded identifier")
    return value


def _sha256(value: object, name: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{name} is not a lowercase SHA-256")
    return value


def _timestamp(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} is invalid")
    result = float(value)
    if not math.isfinite(result) or not 0.0 <= result <= 253_402_300_799.0:
        raise ValueError(f"{name} is invalid")
    return result


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _normalize_sql(value: str) -> str:
    return " ".join(value.replace("\n", " ").split()).strip().lower()


_TABLE_DDL = (
    (
        "manifest",
        """
        CREATE TABLE manifest(
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        ) WITHOUT ROWID
        """,
    ),
    (
        "global_capacity",
        """
        CREATE TABLE global_capacity(
            singleton INTEGER PRIMARY KEY CHECK(singleton=1),
            reservation_rows INTEGER NOT NULL CHECK(reservation_rows>=0),
            reserved_count INTEGER NOT NULL CHECK(reserved_count>=0),
            committed_count INTEGER NOT NULL CHECK(committed_count>=0),
            reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>=0),
            committed_bytes INTEGER NOT NULL CHECK(committed_bytes>=0),
            reserved_entries INTEGER NOT NULL CHECK(reserved_entries>=0),
            committed_entries INTEGER NOT NULL CHECK(committed_entries>=0)
        )
        """,
    ),
    (
        "provider_capacity",
        """
        CREATE TABLE provider_capacity(
            provider TEXT PRIMARY KEY,
            max_bytes INTEGER NOT NULL CHECK(max_bytes>0),
            max_entries INTEGER NOT NULL CHECK(max_entries>0),
            reservation_rows INTEGER NOT NULL CHECK(reservation_rows>=0),
            reserved_count INTEGER NOT NULL CHECK(reserved_count>=0),
            committed_count INTEGER NOT NULL CHECK(committed_count>=0),
            reserved_bytes INTEGER NOT NULL CHECK(reserved_bytes>=0),
            committed_bytes INTEGER NOT NULL CHECK(committed_bytes>=0),
            reserved_entries INTEGER NOT NULL CHECK(reserved_entries>=0),
            committed_entries INTEGER NOT NULL CHECK(committed_entries>=0)
        ) WITHOUT ROWID
        """,
    ),
    (
        "reservations",
        """
        CREATE TABLE reservations(
            reservation_id TEXT PRIMARY KEY,
            provider TEXT NOT NULL REFERENCES provider_capacity(provider),
            job_identity TEXT NOT NULL,
            requested_bytes INTEGER NOT NULL CHECK(requested_bytes>0),
            requested_entries INTEGER NOT NULL CHECK(requested_entries>0),
            request_sha256 TEXT NOT NULL,
            reservation_sha256 TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('RESERVED','COMMITTED')),
            created_at REAL NOT NULL,
            actual_bytes INTEGER,
            actual_entries INTEGER,
            content_sha256 TEXT,
            receipt_sha256 TEXT,
            capture_sha256 TEXT,
            commit_sha256 TEXT,
            committed_at REAL,
            UNIQUE(provider,job_identity),
            CHECK(
                (status='RESERVED' AND actual_bytes IS NULL AND
                 actual_entries IS NULL AND content_sha256 IS NULL AND
                 receipt_sha256 IS NULL AND capture_sha256 IS NULL AND
                 commit_sha256 IS NULL AND committed_at IS NULL)
                OR
                (status='COMMITTED' AND actual_bytes>0 AND
                 actual_entries>0 AND content_sha256 IS NOT NULL AND
                 receipt_sha256 IS NOT NULL AND capture_sha256 IS NOT NULL AND
                 commit_sha256 IS NOT NULL AND committed_at IS NOT NULL)
            )
        ) WITHOUT ROWID
        """,
    ),
)

_INDEX_DDL = (
    (
        "reservations_provider_status",
        "CREATE INDEX reservations_provider_status "
        "ON reservations(provider,status,reservation_id)",
    ),
)

_TRIGGER_DDL = (
    (
        "manifest_no_update",
        "CREATE TRIGGER manifest_no_update BEFORE UPDATE ON manifest "
        "BEGIN SELECT RAISE(ABORT,'manifest is immutable'); END",
    ),
    (
        "manifest_no_delete",
        "CREATE TRIGGER manifest_no_delete BEFORE DELETE ON manifest "
        "BEGIN SELECT RAISE(ABORT,'manifest is immutable'); END",
    ),
    (
        "provider_no_insert",
        "CREATE TRIGGER provider_no_insert BEFORE INSERT ON provider_capacity "
        "WHEN (SELECT count(*) FROM manifest)>0 "
        "BEGIN SELECT RAISE(ABORT,'provider profile is immutable'); END",
    ),
    (
        "provider_no_delete",
        "CREATE TRIGGER provider_no_delete BEFORE DELETE ON provider_capacity "
        "BEGIN SELECT RAISE(ABORT,'provider profile is immutable'); END",
    ),
    (
        "provider_caps_immutable",
        "CREATE TRIGGER provider_caps_immutable BEFORE UPDATE ON provider_capacity "
        "WHEN NEW.provider<>OLD.provider OR NEW.max_bytes<>OLD.max_bytes "
        "OR NEW.max_entries<>OLD.max_entries "
        "BEGIN SELECT RAISE(ABORT,'provider caps are immutable'); END",
    ),
    (
        "global_no_insert",
        "CREATE TRIGGER global_no_insert BEFORE INSERT ON global_capacity "
        "WHEN (SELECT count(*) FROM manifest)>0 "
        "BEGIN SELECT RAISE(ABORT,'global row is immutable'); END",
    ),
    (
        "global_no_delete",
        "CREATE TRIGGER global_no_delete BEFORE DELETE ON global_capacity "
        "BEGIN SELECT RAISE(ABORT,'global row cannot be deleted'); END",
    ),
    (
        "reservation_no_delete",
        "CREATE TRIGGER reservation_no_delete BEFORE DELETE ON reservations "
        "BEGIN SELECT RAISE(ABORT,'capacity is never released'); END",
    ),
    (
        "reservation_transition_only",
        """
        CREATE TRIGGER reservation_transition_only BEFORE UPDATE ON reservations
        WHEN NOT(
            OLD.status='RESERVED' AND NEW.status='COMMITTED' AND
            NEW.reservation_id=OLD.reservation_id AND
            NEW.provider=OLD.provider AND
            NEW.job_identity=OLD.job_identity AND
            NEW.requested_bytes=OLD.requested_bytes AND
            NEW.requested_entries=OLD.requested_entries AND
            NEW.request_sha256=OLD.request_sha256 AND
            NEW.reservation_sha256=OLD.reservation_sha256 AND
            NEW.created_at=OLD.created_at
        )
        BEGIN SELECT RAISE(ABORT,'only RESERVED to COMMITTED is allowed'); END
        """,
    ),
)


def _expected_schema_objects() -> tuple[tuple[str, str, str, str], ...]:
    values: list[tuple[str, str, str, str]] = []
    for name, sql in _TABLE_DDL:
        values.append(("table", name, name, _normalize_sql(sql)))
    for name, sql in _INDEX_DDL:
        values.append(("index", name, "reservations", _normalize_sql(sql)))
    for name, sql in _TRIGGER_DDL:
        table = (
            "manifest" if name.startswith("manifest")
            else "provider_capacity" if name.startswith("provider")
            else "global_capacity" if name.startswith("global")
            else "reservations"
        )
        values.append(("trigger", name, table, _normalize_sql(sql)))
    return tuple(sorted(values))


_EXPECTED_SCHEMA_OBJECTS = _expected_schema_objects()
_EXPECTED_SCHEMA_SHA256 = hashlib.sha256(
    _canonical_json(_EXPECTED_SCHEMA_OBJECTS)
).hexdigest()


class RawCapacityLedger:
    """Cross-process, append-only reservation and commit accounting."""

    def __init__(
        self,
        path: str | Path,
        *,
        profile: RawCapacityProfile,
        clock: Callable[[], float] = time.time,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        supplied = Path(path)
        if not supplied.is_absolute():
            raise ValueError("capacity ledger path must be absolute")
        if not isinstance(profile, RawCapacityProfile):
            raise TypeError("profile must be RawCapacityProfile")
        self.path = supplied
        self.profile = profile
        self._clock = clock
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self.initialize()

    def initialize(self) -> dict[str, int]:
        with self._lock:
            self._prepare_path()
            self._check_catalog_envelope()
            try:
                with closing(self._connect_unconfigured()) as database:
                    database.execute("PRAGMA foreign_keys=ON")
                    database.execute("PRAGMA synchronous=FULL")
                    mode = str(
                        database.execute(
                            f"PRAGMA journal_mode={JOURNAL_MODE}"
                        ).fetchone()[0]
                    ).lower()
                    if mode != JOURNAL_MODE:
                        raise RawCapacityLedgerIntegrityError(
                            "SQLite could not enforce DELETE journaling"
                        )
                    database.execute("PRAGMA temp_store=MEMORY")
                    database.execute("BEGIN IMMEDIATE")
                    try:
                        if not self._table_names(database):
                            self._create_schema(database)
                        self._validate_schema(database)
                        self._fault("initialize.before_commit")
                        database.commit()
                        self._fault("initialize.after_commit")
                    except BaseException:
                        if database.in_transaction:
                            database.rollback()
                        raise
                self._durability_barrier()
            except sqlite3.DatabaseError as exc:
                self._raise_sqlite(exc)
        return self.stats()

    def reserve(
        self,
        *,
        reservation_id: str,
        provider: str,
        job_identity: str,
        requested_bytes: int,
        requested_entries: int,
    ) -> CapacityReservationReceipt:
        reservation = _identifier(reservation_id, "reservation_id")
        provider_id = _identifier(provider, "provider")
        job = _identifier(job_identity, "job_identity")
        requested_byte_count = _positive_integer(
            requested_bytes, "requested_bytes"
        )
        requested_entry_count = _positive_integer(
            requested_entries, "requested_entries"
        )
        request_sha = self._request_sha256(
            reservation_id=reservation,
            provider=provider_id,
            job_identity=job,
            requested_bytes=requested_byte_count,
            requested_entries=requested_entry_count,
        )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                self._fault("reserve.after_begin")
                existing = database.execute(
                    "SELECT * FROM reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
                if existing is not None:
                    receipt = self._validated_reservation(existing)
                    if (
                        receipt.provider != provider_id
                        or receipt.job_identity != job
                        or receipt.requested_bytes != requested_byte_count
                        or receipt.requested_entries != requested_entry_count
                        or receipt.request_sha256 != request_sha
                    ):
                        raise RawCapacityLedgerConflict(
                            "reservation identity was reused with a different request"
                        )
                    database.commit()
                    self._durability_barrier()
                    return receipt
                same_job = database.execute(
                    "SELECT reservation_id FROM reservations "
                    "WHERE provider=? AND job_identity=?",
                    (provider_id, job),
                ).fetchone()
                if same_job is not None:
                    raise RawCapacityLedgerConflict(
                        "provider job identity already has another reservation"
                    )
                global_row = self._global_row(database)
                provider_row = self._provider_row(database, provider_id)
                if int(global_row["reservation_rows"]) >= self.profile.max_reservations:
                    raise RawCapacityLedgerCapacityError(
                        "global reservation row capacity is exhausted"
                    )
                self._check_capacity(
                    global_row,
                    provider_row,
                    add_bytes=requested_byte_count,
                    add_entries=requested_entry_count,
                )
                created_at = _timestamp(self._clock(), "created_at")
                reservation_sha = self._reservation_sha256(
                    request_sha256=request_sha,
                    created_at=created_at,
                )
                database.execute(
                    "INSERT INTO reservations("
                    "reservation_id,provider,job_identity,requested_bytes,"
                    "requested_entries,request_sha256,reservation_sha256,status,"
                    "created_at,actual_bytes,actual_entries,content_sha256,"
                    "receipt_sha256,capture_sha256,commit_sha256,committed_at) "
                    "VALUES(?,?,?,?,?,?,?,'RESERVED',?,NULL,NULL,NULL,NULL,NULL,NULL,NULL)",
                    (
                        reservation,
                        provider_id,
                        job,
                        requested_byte_count,
                        requested_entry_count,
                        request_sha,
                        reservation_sha,
                        created_at,
                    ),
                )
                self._fault("reserve.after_reservation")
                self._increment_reserved(
                    database,
                    provider=provider_id,
                    byte_count=requested_byte_count,
                    entry_count=requested_entry_count,
                )
                self._fault("reserve.after_aggregate")
                self._validate_fast_invariants(database)
                self._fault("reserve.before_commit")
                database.commit()
                self._fault("reserve.after_commit")
                row = database.execute(
                    "SELECT * FROM reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
                result = self._validated_reservation(row)
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc)
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._durability_barrier()
        return result

    def commit(
        self,
        *,
        reservation_id: str,
        actual_bytes: int,
        actual_entries: int,
        content_sha256: str,
        receipt_sha256: str,
        capture_sha256: str,
    ) -> CapacityReservationReceipt:
        reservation = _identifier(reservation_id, "reservation_id")
        actual_byte_count = _positive_integer(actual_bytes, "actual_bytes")
        actual_entry_count = _positive_integer(actual_entries, "actual_entries")
        content_sha = _sha256(content_sha256, "content_sha256")
        receipt_sha = _sha256(receipt_sha256, "receipt_sha256")
        capture_sha = _sha256(capture_sha256, "capture_sha256")
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate_schema(database)
                self._fault("commit.after_begin")
                row = database.execute(
                    "SELECT * FROM reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
                if row is None:
                    raise RawCapacityLedgerConflict(
                        "commit has no durable prior reservation"
                    )
                existing = self._validated_reservation(row)
                if existing.status == "COMMITTED":
                    if (
                        existing.actual_bytes != actual_byte_count
                        or existing.actual_entries != actual_entry_count
                        or existing.content_sha256 != content_sha
                        or existing.receipt_sha256 != receipt_sha
                        or existing.capture_sha256 != capture_sha
                    ):
                        raise RawCapacityLedgerConflict(
                            "committed reservation was retried with different evidence"
                        )
                    database.commit()
                    self._durability_barrier()
                    return existing
                global_row = self._global_row(database)
                provider_row = self._provider_row(database, existing.provider)
                delta_bytes = actual_byte_count - existing.requested_bytes
                delta_entries = actual_entry_count - existing.requested_entries
                if delta_bytes > 0 or delta_entries > 0:
                    self._check_capacity(
                        global_row,
                        provider_row,
                        add_bytes=max(0, delta_bytes),
                        add_entries=max(0, delta_entries),
                    )
                committed_at = _timestamp(self._clock(), "committed_at")
                if committed_at < existing.created_at:
                    raise ValueError("committed_at cannot predate reservation")
                commit_sha = self._commit_sha256(
                    reservation_sha256=existing.reservation_sha256,
                    actual_bytes=actual_byte_count,
                    actual_entries=actual_entry_count,
                    content_sha256=content_sha,
                    receipt_sha256=receipt_sha,
                    capture_sha256=capture_sha,
                    committed_at=committed_at,
                )
                database.execute(
                    "UPDATE reservations SET status='COMMITTED',actual_bytes=?,"
                    "actual_entries=?,content_sha256=?,receipt_sha256=?,"
                    "capture_sha256=?,commit_sha256=?,committed_at=? "
                    "WHERE reservation_id=? AND status='RESERVED'",
                    (
                        actual_byte_count,
                        actual_entry_count,
                        content_sha,
                        receipt_sha,
                        capture_sha,
                        commit_sha,
                        committed_at,
                        reservation,
                    ),
                )
                self._fault("commit.after_reservation")
                self._convert_to_committed(
                    database,
                    provider=existing.provider,
                    requested_bytes=existing.requested_bytes,
                    requested_entries=existing.requested_entries,
                    actual_bytes=actual_byte_count,
                    actual_entries=actual_entry_count,
                )
                self._fault("commit.after_aggregate")
                self._validate_fast_invariants(database)
                self._fault("commit.before_commit")
                database.commit()
                self._fault("commit.after_commit")
                committed_row = database.execute(
                    "SELECT * FROM reservations WHERE reservation_id=?",
                    (reservation,),
                ).fetchone()
                result = self._validated_reservation(committed_row)
            except sqlite3.DatabaseError as exc:
                if database.in_transaction:
                    database.rollback()
                self._raise_sqlite(exc)
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._durability_barrier()
        return result

    def load(self, reservation_id: str) -> CapacityReservationReceipt | None:
        reservation = _identifier(reservation_id, "reservation_id")
        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM reservations WHERE reservation_id=?",
                (reservation,),
            ).fetchone()
            return None if row is None else self._validated_reservation(row)

    def load_for_job(
        self,
        *,
        provider: str,
        job_identity: str,
    ) -> CapacityReservationReceipt | None:
        """Load the unique reservation bound to one provider job identity.

        This read-only lookup exists for crash recovery from durable raw-store
        evidence.  It never creates capacity after bytes already exist and
        therefore cannot turn an unreserved capture into an admitted one.
        """

        provider_id = _identifier(provider, "provider")
        job = _identifier(job_identity, "job_identity")
        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            row = database.execute(
                "SELECT * FROM reservations WHERE provider=? AND job_identity=?",
                (provider_id, job),
            ).fetchone()
            return None if row is None else self._validated_reservation(row)

    def accounting_reservations(
        self,
        *,
        max_rows: int,
    ) -> tuple[CapacityReservationReceipt, ...]:
        """Return a caller-bounded, fully validated reservation inventory."""

        if (
            isinstance(max_rows, bool)
            or not isinstance(max_rows, int)
            or not 1 <= max_rows <= MAX_RESERVATIONS_HARD
        ):
            raise ValueError("invalid accounting reservation bound")
        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            rows = database.execute(
                "SELECT * FROM reservations ORDER BY reservation_id LIMIT ?",
                (max_rows + 1,),
            ).fetchall()
        if len(rows) > max_rows:
            raise RawCapacityLedgerCapacityError(
                "capacity reservation inventory exceeds its recovery bound"
            )
        return tuple(self._validated_reservation(row) for row in rows)

    def stats(self) -> dict[str, int]:
        """Return O(1) durable counters; never scan reservations."""

        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            self._validate_fast_invariants(database)
            row = self._global_row(database)
            result = self._counter_mapping(row)
            catalog_page_size = int(
                database.execute("PRAGMA page_size").fetchone()[0]
            )
            catalog_main_database_bytes = (
                int(database.execute("PRAGMA page_count").fetchone()[0])
                * catalog_page_size
            )
            catalog_main_database_max_bytes = (
                capacity_ledger_catalog_main_database_max_bytes(
                    max_catalog_bytes=self.profile.max_catalog_bytes,
                    page_size_bytes=catalog_page_size,
                )
            )
            if (
                catalog_main_database_bytes
                > catalog_main_database_max_bytes
            ):
                raise RawCapacityLedgerCapacityError(
                    "capacity ledger main database exceeds its envelope"
                )
        result.update({
            "global_max_bytes": self.profile.global_max_bytes,
            "global_max_entries": self.profile.global_max_entries,
            "max_reservations": self.profile.max_reservations,
            "catalog_file_bytes": self._catalog_physical_bytes(),
            "catalog_main_database_bytes": catalog_main_database_bytes,
            "catalog_main_database_max_bytes": (
                catalog_main_database_max_bytes
            ),
            "catalog_page_size_bytes": catalog_page_size,
            "max_catalog_bytes": self.profile.max_catalog_bytes,
            "operational_admission_eligible": 0,
            "directory_fsync_supported": int(DIRECTORY_FSYNC_SUPPORTED),
        })
        return result

    def provider_stats(self, provider: str) -> dict[str, int]:
        provider_id = _identifier(provider, "provider")
        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database)
            self._validate_fast_invariants(database)
            row = self._provider_row(database, provider_id)
            result = self._counter_mapping(row)
            result.update({
                "max_bytes": int(row["max_bytes"]),
                "max_entries": int(row["max_entries"]),
            })
            return result

    def audit(self) -> dict[str, int]:
        """Read-only full internal audit; no filesystem capacity is reclaimed."""

        with self._lock, closing(self._connect_read_only()) as database:
            self._validate_schema(database, full_integrity=True)
            self._validate_fast_invariants(database)
            rows = database.execute(
                "SELECT * FROM reservations ORDER BY reservation_id"
            ).fetchall()
            if len(rows) > self.profile.max_reservations:
                raise RawCapacityLedgerIntegrityError(
                    "reservation rows exceed the immutable profile"
                )
            totals = {
                "reservation_rows": 0,
                "reserved_count": 0,
                "committed_count": 0,
                "reserved_bytes": 0,
                "committed_bytes": 0,
                "reserved_entries": 0,
                "committed_entries": 0,
            }
            provider_totals = {
                item.provider: dict(totals) for item in self.profile.providers
            }
            for row in rows:
                receipt = self._validated_reservation(row)
                totals["reservation_rows"] += 1
                current = provider_totals[receipt.provider]
                current["reservation_rows"] += 1
                if receipt.status == "RESERVED":
                    totals["reserved_count"] += 1
                    totals["reserved_bytes"] += receipt.requested_bytes
                    totals["reserved_entries"] += receipt.requested_entries
                    current["reserved_count"] += 1
                    current["reserved_bytes"] += receipt.requested_bytes
                    current["reserved_entries"] += receipt.requested_entries
                else:
                    assert receipt.actual_bytes is not None
                    assert receipt.actual_entries is not None
                    totals["committed_count"] += 1
                    totals["committed_bytes"] += receipt.actual_bytes
                    totals["committed_entries"] += receipt.actual_entries
                    current["committed_count"] += 1
                    current["committed_bytes"] += receipt.actual_bytes
                    current["committed_entries"] += receipt.actual_entries
            durable_global = self._counter_mapping(self._global_row(database))
            if totals != durable_global:
                raise RawCapacityLedgerIntegrityError(
                    "global aggregate counters do not match reservation facts"
                )
            for provider, expected in provider_totals.items():
                durable = self._counter_mapping(
                    self._provider_row(database, provider)
                )
                if expected != durable:
                    raise RawCapacityLedgerIntegrityError(
                        f"provider aggregate counters do not match: {provider}"
                    )
            catalog_page_size = int(
                database.execute("PRAGMA page_size").fetchone()[0]
            )
            catalog_main_database_bytes = (
                int(database.execute("PRAGMA page_count").fetchone()[0])
                * catalog_page_size
            )
            catalog_main_database_max_bytes = (
                capacity_ledger_catalog_main_database_max_bytes(
                    max_catalog_bytes=self.profile.max_catalog_bytes,
                    page_size_bytes=catalog_page_size,
                )
            )
            if (
                catalog_main_database_bytes
                > catalog_main_database_max_bytes
            ):
                raise RawCapacityLedgerCapacityError(
                    "capacity ledger main database exceeds its envelope"
                )
        self._check_catalog_envelope()
        return {
            **totals,
            "provider_count": len(provider_totals),
            "catalog_file_bytes": self._catalog_physical_bytes(),
            "catalog_main_database_bytes": catalog_main_database_bytes,
            "catalog_main_database_max_bytes": (
                catalog_main_database_max_bytes
            ),
            "catalog_page_size_bytes": catalog_page_size,
            "max_catalog_bytes": self.profile.max_catalog_bytes,
            "automatic_reclaim_performed": 0,
            "operational_admission_eligible": 0,
            "directory_fsync_supported": int(DIRECTORY_FSYNC_SUPPORTED),
        }

    @staticmethod
    def _counter_mapping(row: sqlite3.Row) -> dict[str, int]:
        return {
            name: int(row[name])
            for name in (
                "reservation_rows",
                "reserved_count",
                "committed_count",
                "reserved_bytes",
                "committed_bytes",
                "reserved_entries",
                "committed_entries",
            )
        }

    @staticmethod
    def _request_sha256(
        *,
        reservation_id: str,
        provider: str,
        job_identity: str,
        requested_bytes: int,
        requested_entries: int,
    ) -> str:
        return hashlib.sha256(_canonical_json({
            "schema": RESERVATION_SCHEMA,
            "reservation_id": reservation_id,
            "provider": provider,
            "job_identity": job_identity,
            "requested_bytes": requested_bytes,
            "requested_entries": requested_entries,
        })).hexdigest()

    @staticmethod
    def _reservation_sha256(*, request_sha256: str, created_at: float) -> str:
        return hashlib.sha256(_canonical_json({
            "schema": RESERVATION_SCHEMA,
            "request_sha256": request_sha256,
            "created_at": created_at,
            "status": "RESERVED",
        })).hexdigest()

    @staticmethod
    def _commit_sha256(
        *,
        reservation_sha256: str,
        actual_bytes: int,
        actual_entries: int,
        content_sha256: str,
        receipt_sha256: str,
        capture_sha256: str,
        committed_at: float,
    ) -> str:
        return hashlib.sha256(_canonical_json({
            "schema": COMMIT_SCHEMA,
            "reservation_sha256": reservation_sha256,
            "actual_bytes": actual_bytes,
            "actual_entries": actual_entries,
            "content_sha256": content_sha256,
            "receipt_sha256": receipt_sha256,
            "capture_sha256": capture_sha256,
            "committed_at": committed_at,
        })).hexdigest()

    def _validated_reservation(self, row: sqlite3.Row) -> CapacityReservationReceipt:
        try:
            reservation_id = _identifier(row["reservation_id"], "reservation_id")
            provider = _identifier(row["provider"], "provider")
            job = _identifier(row["job_identity"], "job_identity")
            requested_bytes = _positive_integer(
                int(row["requested_bytes"]), "requested_bytes"
            )
            requested_entries = _positive_integer(
                int(row["requested_entries"]), "requested_entries"
            )
            request_sha = _sha256(row["request_sha256"], "request_sha256")
            expected_request = self._request_sha256(
                reservation_id=reservation_id,
                provider=provider,
                job_identity=job,
                requested_bytes=requested_bytes,
                requested_entries=requested_entries,
            )
            if request_sha != expected_request:
                raise RawCapacityLedgerIntegrityError(
                    "reservation request hash is invalid"
                )
            created_at = _timestamp(row["created_at"], "created_at")
            reservation_sha = _sha256(
                row["reservation_sha256"], "reservation_sha256"
            )
            if reservation_sha != self._reservation_sha256(
                request_sha256=request_sha,
                created_at=created_at,
            ):
                raise RawCapacityLedgerIntegrityError(
                    "reservation receipt hash is invalid"
                )
            status = str(row["status"])
            if status not in _STATUSES:
                raise RawCapacityLedgerIntegrityError(
                    "reservation status is invalid"
                )
            if status == "RESERVED":
                committed_values = (
                    row["actual_bytes"], row["actual_entries"],
                    row["content_sha256"], row["receipt_sha256"],
                    row["capture_sha256"], row["commit_sha256"],
                    row["committed_at"],
                )
                if any(item is not None for item in committed_values):
                    raise RawCapacityLedgerIntegrityError(
                        "reserved row exposes committed evidence"
                    )
                return CapacityReservationReceipt(
                    reservation_id=reservation_id,
                    provider=provider,
                    job_identity=job,
                    requested_bytes=requested_bytes,
                    requested_entries=requested_entries,
                    request_sha256=request_sha,
                    reservation_sha256=reservation_sha,
                    status="RESERVED",
                    created_at=created_at,
                    actual_bytes=None,
                    actual_entries=None,
                    content_sha256=None,
                    receipt_sha256=None,
                    capture_sha256=None,
                    commit_sha256=None,
                    committed_at=None,
                )
            actual_bytes = _positive_integer(int(row["actual_bytes"]), "actual_bytes")
            actual_entries = _positive_integer(
                int(row["actual_entries"]), "actual_entries"
            )
            content_sha = _sha256(row["content_sha256"], "content_sha256")
            receipt_sha = _sha256(row["receipt_sha256"], "receipt_sha256")
            capture_sha = _sha256(row["capture_sha256"], "capture_sha256")
            committed_at = _timestamp(row["committed_at"], "committed_at")
            if committed_at < created_at:
                raise RawCapacityLedgerIntegrityError(
                    "commit predates its reservation"
                )
            commit_sha = _sha256(row["commit_sha256"], "commit_sha256")
            if commit_sha != self._commit_sha256(
                reservation_sha256=reservation_sha,
                actual_bytes=actual_bytes,
                actual_entries=actual_entries,
                content_sha256=content_sha,
                receipt_sha256=receipt_sha,
                capture_sha256=capture_sha,
                committed_at=committed_at,
            ):
                raise RawCapacityLedgerIntegrityError("commit hash is invalid")
            return CapacityReservationReceipt(
                reservation_id=reservation_id,
                provider=provider,
                job_identity=job,
                requested_bytes=requested_bytes,
                requested_entries=requested_entries,
                request_sha256=request_sha,
                reservation_sha256=reservation_sha,
                status="COMMITTED",
                created_at=created_at,
                actual_bytes=actual_bytes,
                actual_entries=actual_entries,
                content_sha256=content_sha,
                receipt_sha256=receipt_sha,
                capture_sha256=capture_sha,
                commit_sha256=commit_sha,
                committed_at=committed_at,
            )
        except RawCapacityLedgerError:
            raise
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise RawCapacityLedgerIntegrityError(
                "reservation row violates its durable contract"
            ) from exc

    def _check_capacity(
        self,
        global_row: sqlite3.Row,
        provider_row: sqlite3.Row,
        *,
        add_bytes: int,
        add_entries: int,
    ) -> None:
        global_bytes = int(global_row["reserved_bytes"]) + int(
            global_row["committed_bytes"]
        )
        global_entries = int(global_row["reserved_entries"]) + int(
            global_row["committed_entries"]
        )
        provider_bytes = int(provider_row["reserved_bytes"]) + int(
            provider_row["committed_bytes"]
        )
        provider_entries = int(provider_row["reserved_entries"]) + int(
            provider_row["committed_entries"]
        )
        if global_bytes + add_bytes > self.profile.global_max_bytes:
            raise RawCapacityLedgerCapacityError("global byte capacity is exhausted")
        if global_entries + add_entries > self.profile.global_max_entries:
            raise RawCapacityLedgerCapacityError("global entry capacity is exhausted")
        if provider_bytes + add_bytes > int(provider_row["max_bytes"]):
            raise RawCapacityLedgerCapacityError("provider byte capacity is exhausted")
        if provider_entries + add_entries > int(provider_row["max_entries"]):
            raise RawCapacityLedgerCapacityError("provider entry capacity is exhausted")

    @staticmethod
    def _increment_reserved(
        database: sqlite3.Connection,
        *,
        provider: str,
        byte_count: int,
        entry_count: int,
    ) -> None:
        database.execute(
            "UPDATE global_capacity SET reservation_rows=reservation_rows+1,"
            "reserved_count=reserved_count+1,reserved_bytes=reserved_bytes+?,"
            "reserved_entries=reserved_entries+? WHERE singleton=1",
            (byte_count, entry_count),
        )
        database.execute(
            "UPDATE provider_capacity SET reservation_rows=reservation_rows+1,"
            "reserved_count=reserved_count+1,reserved_bytes=reserved_bytes+?,"
            "reserved_entries=reserved_entries+? WHERE provider=?",
            (byte_count, entry_count, provider),
        )

    @staticmethod
    def _convert_to_committed(
        database: sqlite3.Connection,
        *,
        provider: str,
        requested_bytes: int,
        requested_entries: int,
        actual_bytes: int,
        actual_entries: int,
    ) -> None:
        for table, where, parameters in (
            (
                "global_capacity",
                "singleton=1",
                (requested_bytes, actual_bytes, requested_entries, actual_entries),
            ),
            (
                "provider_capacity",
                "provider=?",
                (
                    requested_bytes,
                    actual_bytes,
                    requested_entries,
                    actual_entries,
                    provider,
                ),
            ),
        ):
            database.execute(
                f"UPDATE {table} SET reserved_count=reserved_count-1,"
                "committed_count=committed_count+1,"
                "reserved_bytes=reserved_bytes-?,committed_bytes=committed_bytes+?,"
                "reserved_entries=reserved_entries-?,"
                f"committed_entries=committed_entries+? WHERE {where}",
                parameters,
            )

    def _validate_fast_invariants(self, database: sqlite3.Connection) -> None:
        global_row = self._global_row(database)
        values = self._counter_mapping(global_row)
        if (
            values["reservation_rows"]
            != values["reserved_count"] + values["committed_count"]
            or values["reservation_rows"] > self.profile.max_reservations
            or values["reserved_bytes"] + values["committed_bytes"]
            > self.profile.global_max_bytes
            or values["reserved_entries"] + values["committed_entries"]
            > self.profile.global_max_entries
        ):
            raise RawCapacityLedgerIntegrityError(
                "global O(1) counters violate capacity invariants"
            )
        provider_rows = database.execute(
            "SELECT * FROM provider_capacity ORDER BY provider"
        ).fetchall()
        if len(provider_rows) != len(self.profile.providers):
            raise RawCapacityLedgerIntegrityError("provider counter set is incomplete")
        total_rows = 0
        total_reserved_count = 0
        total_committed_count = 0
        total_reserved_bytes = 0
        total_committed_bytes = 0
        total_reserved_entries = 0
        total_committed_entries = 0
        for row in provider_rows:
            provider_values = self._counter_mapping(row)
            if (
                provider_values["reservation_rows"]
                != provider_values["reserved_count"]
                + provider_values["committed_count"]
                or provider_values["reserved_bytes"]
                + provider_values["committed_bytes"] > int(row["max_bytes"])
                or provider_values["reserved_entries"]
                + provider_values["committed_entries"] > int(row["max_entries"])
            ):
                raise RawCapacityLedgerIntegrityError(
                    f"provider O(1) counters violate invariants: {row['provider']}"
                )
            total_rows += provider_values["reservation_rows"]
            total_reserved_count += provider_values["reserved_count"]
            total_committed_count += provider_values["committed_count"]
            total_reserved_bytes += provider_values["reserved_bytes"]
            total_committed_bytes += provider_values["committed_bytes"]
            total_reserved_entries += provider_values["reserved_entries"]
            total_committed_entries += provider_values["committed_entries"]
        if (
            total_rows != values["reservation_rows"]
            or total_reserved_count != values["reserved_count"]
            or total_committed_count != values["committed_count"]
            or total_reserved_bytes != values["reserved_bytes"]
            or total_committed_bytes != values["committed_bytes"]
            or total_reserved_entries != values["reserved_entries"]
            or total_committed_entries != values["committed_entries"]
        ):
            raise RawCapacityLedgerIntegrityError(
                "provider O(1) counters do not sum to global counters"
            )

    @staticmethod
    def _global_row(database: sqlite3.Connection) -> sqlite3.Row:
        row = database.execute(
            "SELECT * FROM global_capacity WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise RawCapacityLedgerIntegrityError("global counter row is missing")
        return row

    @staticmethod
    def _provider_row(database: sqlite3.Connection, provider: str) -> sqlite3.Row:
        row = database.execute(
            "SELECT * FROM provider_capacity WHERE provider=?", (provider,)
        ).fetchone()
        if row is None:
            raise RawCapacityLedgerCapacityError(
                "provider has no immutable capacity allocation"
            )
        return row

    def _profile_json(self) -> str:
        return _canonical_json(self.profile.to_mapping()).decode("utf-8")

    def _profile_sha256(self) -> str:
        return hashlib.sha256(self._profile_json().encode("utf-8")).hexdigest()

    def _create_schema(self, database: sqlite3.Connection) -> None:
        # Create counters and profile rows before their insert-protection
        # triggers exist.
        for _name, sql in _TABLE_DDL:
            database.execute(sql)
        for _name, sql in _INDEX_DDL:
            database.execute(sql)
        database.execute(
            "INSERT INTO global_capacity VALUES(1,0,0,0,0,0,0,0)"
        )
        for item in self.profile.providers:
            database.execute(
                "INSERT INTO provider_capacity VALUES(?,?,?,0,0,0,0,0,0,0)",
                (item.provider, item.max_bytes, item.max_entries),
            )
        manifest = (
            ("schema_version", str(SCHEMA_VERSION)),
            ("contract", LEDGER_CONTRACT),
            ("algorithm_version", ALGORITHM_VERSION),
            ("admission_status", ADMISSION_STATUS),
            ("journal_mode", JOURNAL_MODE),
            ("sqlite_schema_sha256", _EXPECTED_SCHEMA_SHA256),
            ("profile_json", self._profile_json()),
            ("profile_sha256", self._profile_sha256()),
        )
        database.executemany("INSERT INTO manifest(key,value) VALUES(?,?)", manifest)
        for _name, sql in _TRIGGER_DDL:
            database.execute(sql)

    @staticmethod
    def _table_names(database: sqlite3.Connection) -> set[str]:
        return {
            str(row[0])
            for row in database.execute(
                "SELECT name FROM sqlite_schema WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'"
            )
        }

    @staticmethod
    def _schema_objects(
        database: sqlite3.Connection,
    ) -> tuple[tuple[str, str, str, str], ...]:
        rows = database.execute(
            "SELECT type,name,tbl_name,sql FROM sqlite_schema "
            "WHERE type IN ('table','index','trigger') "
            "AND name NOT LIKE 'sqlite_%' ORDER BY type,name"
        ).fetchall()
        return tuple(sorted(
            (
                str(row[0]),
                str(row[1]),
                str(row[2]),
                _normalize_sql(str(row[3])),
            )
            for row in rows
        ))

    def _validate_schema(
        self,
        database: sqlite3.Connection,
        *,
        full_integrity: bool = False,
    ) -> None:
        actual_schema = self._schema_objects(database)
        if actual_schema != _EXPECTED_SCHEMA_OBJECTS:
            # The reclamation participant is an explicit same-database
            # extension so its tombstone and source removal commit atomically.
            # Strip only its closed object-name catalogue before comparing the
            # immutable v1 ledger fingerprint, then validate the extension in
            # full.  Unknown or partial objects remain fatal.
            from .acquisition_control_reclamation import (
                CONTROL_RECLAMATION_SCHEMA_OBJECT_NAMES,
                AcquisitionControlReclamationError,
                validate_control_reclamation_extension_schema,
            )

            base_schema = tuple(
                item
                for item in actual_schema
                if item[1] not in CONTROL_RECLAMATION_SCHEMA_OBJECT_NAMES
            )
            if base_schema != _EXPECTED_SCHEMA_OBJECTS:
                raise RawCapacityLedgerIntegrityError(
                    "capacity ledger schema fingerprint is invalid"
                )
            try:
                validate_control_reclamation_extension_schema(
                    database,
                    expected_role="capacity_ledger",
                )
            except AcquisitionControlReclamationError as exc:
                raise RawCapacityLedgerIntegrityError(
                    "capacity reclamation extension is invalid"
                ) from exc
        manifest = {
            str(row[0]): str(row[1])
            for row in database.execute("SELECT key,value FROM manifest")
        }
        expected = {
            "schema_version": str(SCHEMA_VERSION),
            "contract": LEDGER_CONTRACT,
            "algorithm_version": ALGORITHM_VERSION,
            "admission_status": ADMISSION_STATUS,
            "journal_mode": JOURNAL_MODE,
            "sqlite_schema_sha256": _EXPECTED_SCHEMA_SHA256,
            "profile_json": self._profile_json(),
            "profile_sha256": self._profile_sha256(),
        }
        if manifest != expected:
            raise RawCapacityLedgerIntegrityError(
                "capacity ledger manifest or immutable profile is invalid"
            )
        provider_profile = tuple(
            (str(row[0]), int(row[1]), int(row[2]))
            for row in database.execute(
                "SELECT provider,max_bytes,max_entries FROM provider_capacity "
                "ORDER BY provider"
            )
        )
        expected_provider_profile = tuple(
            (item.provider, item.max_bytes, item.max_entries)
            for item in self.profile.providers
        )
        if provider_profile != expected_provider_profile:
            raise RawCapacityLedgerIntegrityError(
                "provider capacity profile is invalid"
            )
        if full_integrity:
            quick = database.execute("PRAGMA quick_check(1)").fetchone()
            if quick is None or str(quick[0]).lower() != "ok":
                raise RawCapacityLedgerIntegrityError(
                    "capacity ledger quick_check failed"
                )
            if database.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise RawCapacityLedgerIntegrityError(
                    "capacity ledger foreign_key_check failed"
                )

    def _prepare_path(self) -> None:
        parent = self.path.parent
        if self.path.exists() and self.path.is_symlink():
            raise RawCapacityLedgerIntegrityError("ledger path is a symlink")
        parent.mkdir(parents=True, exist_ok=True)
        if parent.is_symlink() or not parent.is_dir():
            raise RawCapacityLedgerIntegrityError("ledger parent is unsafe")
        self._check_catalog_envelope()

    def _main_page_limit(self, page_size: int) -> int:
        maximum_bytes = capacity_ledger_catalog_main_database_max_bytes(
            max_catalog_bytes=self.profile.max_catalog_bytes,
            page_size_bytes=page_size,
        )
        return maximum_bytes // page_size

    def _connect_unconfigured(self) -> sqlite3.Connection:
        self._check_catalog_envelope()
        try:
            database = sqlite3.connect(
                self.path,
                timeout=float(self.profile.sqlite_busy_timeout_seconds),
            )
        except sqlite3.DatabaseError as exc:
            self._raise_sqlite(exc)
        database.row_factory = sqlite3.Row
        try:
            database.execute(
                f"PRAGMA busy_timeout={max(1, int(self.profile.sqlite_busy_timeout_seconds * 1000))}"
            )
            page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
            page_limit = self._main_page_limit(page_size)
            if int(database.execute("PRAGMA page_count").fetchone()[0]) > page_limit:
                raise RawCapacityLedgerCapacityError(
                    "capacity ledger main database exceeds its envelope"
                )
            applied = int(
                database.execute(f"PRAGMA max_page_count={page_limit}").fetchone()[0]
            )
            if applied != page_limit:
                raise RawCapacityLedgerCapacityError(
                    "SQLite could not enforce the main database envelope"
                )
            return database
        except BaseException:
            database.close()
            raise

    def _connect(self) -> sqlite3.Connection:
        database = self._connect_unconfigured()
        try:
            database.execute("PRAGMA foreign_keys=ON")
            mode = str(
                database.execute(f"PRAGMA journal_mode={JOURNAL_MODE}").fetchone()[0]
            ).lower()
            if mode != JOURNAL_MODE:
                raise RawCapacityLedgerIntegrityError(
                    "SQLite could not enforce DELETE journaling"
                )
            database.execute("PRAGMA synchronous=FULL")
            database.execute("PRAGMA temp_store=MEMORY")
            return database
        except BaseException:
            database.close()
            raise

    def _connect_read_only(self) -> sqlite3.Connection:
        self._check_catalog_envelope()
        if not self.path.exists() or self.path.is_symlink():
            raise RawCapacityLedgerIntegrityError("capacity ledger is unavailable")
        try:
            database = sqlite3.connect(
                self.path.resolve().as_uri() + "?mode=ro",
                uri=True,
                timeout=float(self.profile.sqlite_busy_timeout_seconds),
            )
        except sqlite3.DatabaseError as exc:
            self._raise_sqlite(exc)
        database.row_factory = sqlite3.Row
        try:
            database.execute("PRAGMA query_only=ON")
            database.execute("PRAGMA foreign_keys=ON")
            database.execute(
                f"PRAGMA busy_timeout={max(1, int(self.profile.sqlite_busy_timeout_seconds * 1000))}"
            )
            self._check_catalog_envelope()
            return database
        except BaseException:
            database.close()
            raise

    @staticmethod
    def _regular_file_size(path: Path, name: str) -> int:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            return 0
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
            raise RawCapacityLedgerIntegrityError(f"unsafe ledger {name} file")
        return int(metadata.st_size)

    def _catalog_physical_bytes(self) -> int:
        wal = Path(str(self.path) + "-wal")
        shm = Path(str(self.path) + "-shm")
        if wal.exists() or shm.exists():
            raise RawCapacityLedgerIntegrityError(
                "WAL/SHM sidecars are forbidden for the ledger"
            )
        return self._regular_file_size(self.path, "main") + self._regular_file_size(
            Path(str(self.path) + "-journal"), "journal"
        )

    def _check_catalog_envelope(self) -> None:
        if self._catalog_physical_bytes() > self.profile.max_catalog_bytes:
            raise RawCapacityLedgerCapacityError(
                "capacity ledger physical catalog envelope is exhausted"
            )

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        if os.name != "posix":
            return
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def _durability_barrier(self) -> None:
        self._check_catalog_envelope()
        if self.path.exists():
            # Windows rejects fsync on a read-only CRT descriptor.  Opening
            # the existing catalog read/write performs no write by itself and
            # lets fsync provide the intended file barrier.  POSIX retains the
            # narrower read-only descriptor and adds parent-directory fsync.
            descriptor = os.open(
                self.path,
                os.O_RDWR if os.name == "nt" else os.O_RDONLY,
            )
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        self._fsync_directory(self.path.parent)
        self._check_catalog_envelope()

    def _raise_sqlite(self, exc: sqlite3.DatabaseError) -> None:
        message = str(exc).lower()
        if "full" in message or "disk is full" in message:
            raise RawCapacityLedgerCapacityError(
                "capacity ledger SQLite catalog is full"
            ) from exc
        if "locked" in message or "busy" in message:
            raise RawCapacityLedgerBusyError(
                "capacity ledger writer lock exceeded its bounded wait"
            ) from exc
        raise RawCapacityLedgerIntegrityError(
            "capacity ledger SQLite operation failed closed"
        ) from exc

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)


__all__ = [
    "ADMISSION_STATUS",
    "ALGORITHM_VERSION",
    "COMMIT_SCHEMA",
    "CapacityReservationReceipt",
    "DIRECTORY_FSYNC_POLICY",
    "DIRECTORY_FSYNC_SUPPORTED",
    "JOURNAL_MODE",
    "LEDGER_CONTRACT",
    "MAX_PROVIDERS",
    "MAX_RESERVATIONS_HARD",
    "PROFILE_SCHEMA",
    "ProviderCapacityLimit",
    "RESERVATION_SCHEMA",
    "RawCapacityLedger",
    "RawCapacityLedgerBusyError",
    "RawCapacityLedgerCapacityError",
    "RawCapacityLedgerConflict",
    "RawCapacityLedgerError",
    "RawCapacityLedgerIntegrityError",
    "RawCapacityProfile",
    "SCHEMA_VERSION",
    "capacity_ledger_catalog_cycle_reserve_bytes",
    "capacity_ledger_catalog_main_database_max_bytes",
]
