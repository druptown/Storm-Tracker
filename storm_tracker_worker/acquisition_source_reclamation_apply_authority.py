"""Inert durable authorities for acquisition-source reclamation v2.

This module is deliberately not wired into the worker service.  It provides a
sealed, metadata-reference-only candidate catalogue and a separate SQLite
coordinator journal.  Source participants keep their own transactions; the
coordinator only records exact receipts and physically holds COMMITTED or
fully proved APPLIED-replay authority while a participant validates an
idempotent retry, or ABORTING authority while a participant commits.

The older reclamation-v1 PREPARED evidence is neither migrated nor interpreted
as positive evidence here.
"""

from __future__ import annotations

from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence
import errno
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import stat
import threading
import time

from .acquisition_source_reclamation_protocol import (
    ABORTED,
    ABORTING,
    APPLIED,
    COMMITTED,
    PARTICIPANT_APPLY_ORDER,
    PREPARED,
    PREPARING,
    AcquisitionSourceReclamationProtocolConflict,
    AcquisitionSourceReclamationProtocolIntegrityError,
    CommittedReclamationDecisionReceiptV1,
    CoordinatorAbortDecisionReceiptV1,
    HeldCommittedDecisionAuthority,
    ParticipantPrepareBindingV1,
    SourceLocalAbortReceiptV1,
    SourceLocalApplyReceiptV1,
    SourceLocalPrepareReceiptV2,
    SourceReclamationCandidateAuthority,
    SourceReclamationCandidateBindingV1,
    SourceReclamationCandidatePageV1,
    SourceReclamationParticipantV2,
    SourceReclamationRequestV2,
    archive_binding_root,
    candidate_binding_root,
    candidate_participant_view_sha256,
    canonical_bytes,
    digest_mapping,
    require_count,
    require_identifier,
    require_role,
    require_sha256,
    require_timestamp,
)


CANDIDATE_CATALOG_CONTRACT = (
    "stv3-worker-acquisition-source-reclamation-candidate-catalog-v1"
)
CANDIDATE_CATALOG_SCHEMA_VERSION = 1
CANDIDATE_CATALOG_LIMITS_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-limits-v1"
)
CANDIDATE_CATALOG_IDENTITY_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-store-identity-v1"
)
CANDIDATE_AUTHORITY_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-authority-v1"
)
CANDIDATE_SEAL_RECEIPT_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-candidate-seal-v1"
)
COORDINATOR_STORE_CONTRACT = (
    "stv3-worker-acquisition-source-reclamation-apply-coordinator-v2"
)
COORDINATOR_SCHEMA_VERSION = 2
COORDINATOR_LIMITS_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-coordinator-limits-v2"
)
COORDINATOR_STORE_IDENTITY_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-coordinator-identity-v2"
)
COORDINATOR_AUTHORITY_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-decision-authority-v2"
)
COORDINATOR_EVENT_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-coordinator-event-v2"
)
COORDINATOR_SNAPSHOT_SCHEMA = (
    "stv3-worker-acquisition-source-reclamation-coordinator-snapshot-v2"
)

_ZERO_SHA256 = "0" * 64
_MAX_TIMESTAMP = 32_503_680_000.0


class AcquisitionSourceReclamationApplyAuthorityError(RuntimeError):
    """Base error for the inert v2 candidate/coordinator authorities."""


class AcquisitionSourceReclamationApplyIntegrityError(
    AcquisitionSourceReclamationApplyAuthorityError
):
    """A durable row, checksum, schema, path, or authority differs."""


class AcquisitionSourceReclamationApplyConflict(
    AcquisitionSourceReclamationApplyAuthorityError
):
    """An idempotency key or legal state was reused incompatibly."""


class AcquisitionSourceReclamationApplyCapacityError(
    AcquisitionSourceReclamationApplyAuthorityError
):
    """A configured request, record, event, byte, or SQLite cap was reached."""


class AcquisitionSourceReclamationApplyDurabilityError(
    AcquisitionSourceReclamationApplyAuthorityError
):
    """A FULL/DELETE commit or directory durability barrier failed."""


class AcquisitionSourceReclamationApplyClockError(
    AcquisitionSourceReclamationApplyIntegrityError
):
    """The coordinator wall clock moved behind its durable high-water mark."""


def _canonical(value: object) -> bytes:
    try:
        payload = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeError, RecursionError) as exc:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "reclamation authority value is not canonical JSON"
        ) from exc
    if len(payload) > 512 * 1024:
        raise AcquisitionSourceReclamationApplyCapacityError(
            "reclamation authority value exceeds its canonical byte cap"
        )
    return payload


def _digest(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _decode_canonical(payload: object, *, label: str) -> dict[str, Any]:
    try:
        raw = bytes(payload)
        value = json.loads(raw.decode("ascii"))
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} is corrupt"
        ) from exc
    if not isinstance(value, dict) or _canonical(value) != raw:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} is not a canonical mapping"
        )
    return value


def _sqlite_exact_row_bytes(
    row: sqlite3.Row,
    columns: Sequence[str],
) -> bytes:
    """Encode one SQLite row without normalizing any durable value.

    This is an in-process hold witness, not a serialized protocol contract.
    BLOB payloads are retained byte-for-byte; REAL values use their exact
    hexadecimal representation and every cell is length-delimited.
    """

    encoded = bytearray()
    for column in columns:
        value = row[column]
        if value is None:
            cell = b"N"
        elif isinstance(value, bytes):
            cell = b"B" + value
        elif isinstance(value, str):
            cell = b"T" + value.encode("utf-8")
        elif isinstance(value, int):
            cell = b"I" + str(value).encode("ascii")
        elif isinstance(value, float):
            cell = b"R" + value.hex().encode("ascii")
        else:  # pragma: no cover - sqlite3 returns only the types above
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator hold row contains an unsupported SQLite value"
            )
        encoded.extend(len(cell).to_bytes(8, "big"))
        encoded.extend(cell)
    return bytes(encoded)


def _finite_timestamp(value: object, *, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0.0 <= float(value) <= _MAX_TIMESTAMP
    ):
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} is invalid"
        )
    return float(value)


def _normalize_sql(sql: object) -> str:
    if not isinstance(sql, str):
        return ""
    return " ".join(sql.lower().split())


def _canonical_path(path: Path | str, *, label: str) -> Path:
    candidate = Path(path)
    if not candidate.is_absolute():
        raise ValueError(f"{label} must be absolute")
    if candidate.exists() and candidate.is_symlink():
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} is a symlink"
        )
    probe = candidate.parent
    while True:
        if probe.exists() and probe.is_symlink():
            raise AcquisitionSourceReclamationApplyIntegrityError(
                f"{label} has a symlink ancestor"
            )
        if probe.parent == probe:
            break
        probe = probe.parent
    return candidate.resolve(strict=False)


def _path_sha256(path: Path) -> str:
    normalized = os.path.normcase(str(path))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


@dataclass(frozen=True, slots=True)
class _FileWitness:
    device: int
    inode: int
    mode: int
    links: int

    def as_dict(self) -> dict[str, int]:
        return {
            "device": self.device,
            "inode": self.inode,
            "links": self.links,
            "mode": self.mode,
        }


def _file_witness(path: Path, *, label: str) -> _FileWitness:
    if path.is_symlink():
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} is a symlink"
        )
    try:
        result = path.stat()
    except OSError as exc:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} is unavailable"
        ) from exc
    if not stat.S_ISREG(result.st_mode) or int(result.st_nlink) != 1:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} is not a single-link regular file"
        )
    return _FileWitness(
        device=int(result.st_dev),
        inode=int(result.st_ino),
        mode=int(result.st_mode),
        links=int(result.st_nlink),
    )


def _assert_file_witness(
    path: Path,
    expected: _FileWitness,
    *,
    label: str,
) -> None:
    if _file_witness(path, label=label) != expected:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            f"{label} instance changed"
        )


def _default_directory_fsync(path: Path) -> None:
    if os.name == "nt":
        raise AcquisitionSourceReclamationApplyDurabilityError(
            "directory fsync is unavailable on this platform"
        )
    descriptor: int | None = None
    try:
        descriptor = os.open(
            str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        )
        os.fsync(descriptor)
    except OSError as exc:
        if exc.errno in {errno.ENOSPC, errno.EDQUOT}:
            raise AcquisitionSourceReclamationApplyCapacityError(
                "directory fsync ran out of space"
            ) from exc
        raise AcquisitionSourceReclamationApplyDurabilityError(
            "directory fsync failed"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _configure_database(
    database: sqlite3.Connection,
    *,
    busy_timeout_seconds: float,
) -> None:
    database.row_factory = sqlite3.Row
    database.execute("PRAGMA foreign_keys=ON")
    database.execute("PRAGMA trusted_schema=OFF")
    database.execute("PRAGMA synchronous=FULL")
    database.execute(
        f"PRAGMA busy_timeout={int(busy_timeout_seconds * 1000.0)}"
    )
    journal = str(database.execute("PRAGMA journal_mode=DELETE").fetchone()[0])
    if journal.lower() != "delete":
        raise AcquisitionSourceReclamationApplyDurabilityError(
            "reclamation authority requires DELETE journal mode"
        )


def _database_size(database: sqlite3.Connection) -> int:
    page_size = int(database.execute("PRAGMA page_size").fetchone()[0])
    page_count = int(database.execute("PRAGMA page_count").fetchone()[0])
    return page_size * page_count


@dataclass(frozen=True, slots=True)
class SourceReclamationCandidateCatalogLimits:
    max_requests: int = 4096
    max_candidates_per_request: int = 4096
    max_total_records: int = 65_536
    max_database_bytes: int = 128 * 1024 * 1024
    busy_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        require_count(self.max_requests, "candidate request cap", maximum=2**31 - 1)
        require_count(
            self.max_candidates_per_request,
            "candidate per-request cap",
            maximum=4096,
        )
        require_count(
            self.max_total_records,
            "candidate total record cap",
            maximum=2**31 - 1,
        )
        if self.max_requests < 1 or self.max_candidates_per_request < 1:
            raise ValueError("candidate catalogue limits must be positive")
        if self.max_total_records < self.max_candidates_per_request:
            raise ValueError("candidate total record cap is too small")
        if not 1024 * 1024 <= self.max_database_bytes <= 16 * 1024**3:
            raise ValueError("candidate database byte cap is invalid")
        if (
            not math.isfinite(float(self.busy_timeout_seconds))
            or not 0.001 <= float(self.busy_timeout_seconds) <= 60.0
        ):
            raise ValueError("candidate busy timeout is invalid")

    def as_dict(self) -> dict[str, Any]:
        return {
            "busy_timeout_seconds": float(self.busy_timeout_seconds),
            "max_candidates_per_request": self.max_candidates_per_request,
            "max_database_bytes": self.max_database_bytes,
            "max_requests": self.max_requests,
            "max_total_records": self.max_total_records,
            "schema": CANDIDATE_CATALOG_LIMITS_SCHEMA,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class CandidateCatalogSealReceiptV1:
    receipt_sha256: str
    request_sha256: str
    candidate_authority_sha256: str
    candidate_bindings_sha256: str
    archive_binding_root_sha256: str
    candidate_count: int
    sealed_at: float

    def __post_init__(self) -> None:
        for value, label in (
            (self.receipt_sha256, "candidate seal receipt SHA-256"),
            (self.request_sha256, "candidate seal request SHA-256"),
            (self.candidate_authority_sha256, "candidate seal authority SHA-256"),
            (self.candidate_bindings_sha256, "candidate seal inventory SHA-256"),
            (self.archive_binding_root_sha256, "candidate seal archive SHA-256"),
        ):
            require_sha256(value, label)
        require_count(self.candidate_count, "candidate seal count", maximum=4096)
        require_timestamp(self.sealed_at, "candidate seal clock")
        if self.candidate_count < 1 or self.receipt_sha256 != _digest(self._core()):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate seal receipt differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "archive_binding_root_sha256": self.archive_binding_root_sha256,
            "candidate_authority_sha256": self.candidate_authority_sha256,
            "candidate_bindings_sha256": self.candidate_bindings_sha256,
            "candidate_count": self.candidate_count,
            "request_sha256": self.request_sha256,
            "schema": CANDIDATE_SEAL_RECEIPT_SCHEMA,
            "sealed_at": float(self.sealed_at),
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "receipt_sha256": self.receipt_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        candidate_authority_sha256: str,
        candidate_bindings_sha256: str,
        archive_binding_root_sha256: str,
        candidate_count: int,
        sealed_at: float,
    ) -> "CandidateCatalogSealReceiptV1":
        core = {
            "archive_binding_root_sha256": archive_binding_root_sha256,
            "candidate_authority_sha256": candidate_authority_sha256,
            "candidate_bindings_sha256": candidate_bindings_sha256,
            "candidate_count": candidate_count,
            "request_sha256": request_sha256,
            "schema": CANDIDATE_SEAL_RECEIPT_SCHEMA,
            "sealed_at": float(sealed_at),
        }
        return cls(receipt_sha256=_digest(core), **{
            key: value for key, value in core.items() if key != "schema"
        })


_CANDIDATE_TABLE_DDL = (
    """
    CREATE TABLE candidate_catalog_meta(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        store_contract TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        store_instance_sha256 TEXT NOT NULL,
        store_identity_sha256 TEXT NOT NULL,
        authority_sha256 TEXT NOT NULL,
        source_binding_sha256 TEXT NOT NULL,
        archive_store_identity_sha256 TEXT NOT NULL,
        limits_sha256 TEXT NOT NULL,
        canonical_path_sha256 TEXT NOT NULL,
        file_device INTEGER NOT NULL,
        file_inode INTEGER NOT NULL,
        file_mode INTEGER NOT NULL,
        file_links INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE candidate_requests(
        request_sha256 TEXT PRIMARY KEY,
        request_json BLOB NOT NULL,
        candidate_bindings_sha256 TEXT NOT NULL,
        archive_binding_root_sha256 TEXT NOT NULL,
        sealed_archive_anchor_sha256 TEXT NOT NULL,
        candidate_count INTEGER NOT NULL CHECK(candidate_count BETWEEN 1 AND 4096),
        seal_receipt_sha256 TEXT NOT NULL,
        seal_receipt_json BLOB NOT NULL,
        sealed_at REAL NOT NULL
    )
    """,
    """
    CREATE TABLE candidate_records(
        request_sha256 TEXT NOT NULL,
        ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 4095),
        candidate_sha256 TEXT NOT NULL,
        binding_sha256 TEXT NOT NULL,
        binding_json BLOB NOT NULL,
        reference_sha256 TEXT NOT NULL,
        reference_json BLOB NOT NULL,
        PRIMARY KEY(request_sha256,ordinal),
        UNIQUE(request_sha256,candidate_sha256),
        FOREIGN KEY(request_sha256) REFERENCES candidate_requests(request_sha256)
    )
    """,
)

_CANDIDATE_TRIGGER_DDL = (
    "CREATE TRIGGER candidate_meta_no_update BEFORE UPDATE ON candidate_catalog_meta BEGIN SELECT RAISE(ABORT,'candidate_meta_immutable'); END",
    "CREATE TRIGGER candidate_meta_no_delete BEFORE DELETE ON candidate_catalog_meta BEGIN SELECT RAISE(ABORT,'candidate_meta_immutable'); END",
    "CREATE TRIGGER candidate_requests_no_update BEFORE UPDATE ON candidate_requests BEGIN SELECT RAISE(ABORT,'candidate_request_immutable'); END",
    "CREATE TRIGGER candidate_requests_no_delete BEFORE DELETE ON candidate_requests BEGIN SELECT RAISE(ABORT,'candidate_request_immutable'); END",
    "CREATE TRIGGER candidate_records_no_update BEFORE UPDATE ON candidate_records BEGIN SELECT RAISE(ABORT,'candidate_record_immutable'); END",
    "CREATE TRIGGER candidate_records_no_delete BEFORE DELETE ON candidate_records BEGIN SELECT RAISE(ABORT,'candidate_record_immutable'); END",
)

_CANDIDATE_COLUMNS = {
    "candidate_catalog_meta": (
        "singleton", "store_contract", "schema_version", "store_instance_sha256",
        "store_identity_sha256", "authority_sha256", "source_binding_sha256",
        "archive_store_identity_sha256", "limits_sha256", "canonical_path_sha256",
        "file_device", "file_inode", "file_mode", "file_links",
    ),
    "candidate_requests": (
        "request_sha256", "request_json", "candidate_bindings_sha256",
        "archive_binding_root_sha256", "sealed_archive_anchor_sha256",
        "candidate_count", "seal_receipt_sha256", "seal_receipt_json", "sealed_at",
    ),
    "candidate_records": (
        "request_sha256", "ordinal", "candidate_sha256", "binding_sha256",
        "binding_json", "reference_sha256", "reference_json",
    ),
}


class SqliteSourceReclamationCandidateAuthority(
    SourceReclamationCandidateAuthority
):
    """Physical append-only v2 metadata-reference catalogue authority."""

    def __init__(
        self,
        catalog_path: Path | str,
        *,
        expected_store_identity_sha256: str,
        limits: SourceReclamationCandidateCatalogLimits | None = None,
        clock: Callable[[], float] = time.time,
        directory_fsync: Callable[[Path], None] | None = None,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = _canonical_path(catalog_path, label="candidate catalogue path")
        self._expected_store_identity_sha256 = require_sha256(
            expected_store_identity_sha256,
            "candidate catalogue expected identity SHA-256",
        )
        self._limits = limits or SourceReclamationCandidateCatalogLimits()
        if not callable(clock):
            raise TypeError("candidate catalogue clock must be callable")
        if directory_fsync is not None and not callable(directory_fsync):
            raise TypeError("candidate catalogue directory_fsync must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("candidate catalogue fault injector must be callable")
        self._clock = clock
        self._directory_fsync = directory_fsync or _default_directory_fsync
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._witness = _file_witness(self.path, label="candidate catalogue")
        with closing(self._connect()) as database:
            meta = self._validate(database)
        if not hmac.compare_digest(
            str(meta["store_identity_sha256"]),
            self._expected_store_identity_sha256,
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue store identity differs"
            )
        self._store_instance_sha256 = require_sha256(
            str(meta["store_instance_sha256"]),
            "candidate catalogue instance SHA-256",
        )
        self._source_binding_sha256 = require_sha256(
            str(meta["source_binding_sha256"]),
            "candidate catalogue source binding SHA-256",
        )
        self._archive_store_identity_sha256 = require_sha256(
            str(meta["archive_store_identity_sha256"]),
            "candidate archive store identity SHA-256",
        )
        super().__init__(str(meta["authority_sha256"]))

    @classmethod
    def create(
        cls,
        catalog_path: Path | str,
        *,
        source_binding_sha256: str,
        archive_store_identity_sha256: str,
        limits: SourceReclamationCandidateCatalogLimits | None = None,
        clock: Callable[[], float] = time.time,
        directory_fsync: Callable[[Path], None] | None = None,
        fault_injector: Callable[[str], None] | None = None,
    ) -> "SqliteSourceReclamationCandidateAuthority":
        path = _canonical_path(catalog_path, label="candidate catalogue path")
        source_binding = require_sha256(
            source_binding_sha256, "candidate source binding SHA-256"
        )
        archive_identity = require_sha256(
            archive_store_identity_sha256, "candidate archive identity SHA-256"
        )
        selected_limits = limits or SourceReclamationCandidateCatalogLimits()
        barrier = directory_fsync or _default_directory_fsync
        if path.exists() or path.is_symlink():
            raise AcquisitionSourceReclamationApplyConflict(
                "candidate catalogue already exists"
            )
        if not path.parent.exists() or not path.parent.is_dir():
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue parent is unavailable"
            )
        try:
            database = sqlite3.connect(
                path,
                isolation_level=None,
                timeout=selected_limits.busy_timeout_seconds,
            )
            with closing(database):
                _configure_database(
                    database,
                    busy_timeout_seconds=selected_limits.busy_timeout_seconds,
                )
                database.execute("PRAGMA user_version=1")
                database.execute("BEGIN IMMEDIATE")
                for statement in _CANDIDATE_TABLE_DDL:
                    database.execute(statement)
                for statement in _CANDIDATE_TRIGGER_DDL:
                    database.execute(statement)
                witness = _file_witness(path, label="candidate catalogue")
                instance = secrets.token_hex(32)
                identity_core = {
                    "archive_store_identity_sha256": archive_identity,
                    "canonical_path_sha256": _path_sha256(path),
                    "file_witness": witness.as_dict(),
                    "limits_sha256": selected_limits.fingerprint,
                    "schema": CANDIDATE_CATALOG_IDENTITY_SCHEMA,
                    "source_binding_sha256": source_binding,
                    "store_contract": CANDIDATE_CATALOG_CONTRACT,
                    "store_instance_sha256": instance,
                }
                store_identity = _digest(identity_core)
                authority = _digest({
                    "canonical_path_sha256": _path_sha256(path),
                    "schema": CANDIDATE_AUTHORITY_SCHEMA,
                    "store_identity_sha256": store_identity,
                })
                database.execute(
                    "INSERT INTO candidate_catalog_meta VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        CANDIDATE_CATALOG_CONTRACT,
                        CANDIDATE_CATALOG_SCHEMA_VERSION,
                        instance,
                        store_identity,
                        authority,
                        source_binding,
                        archive_identity,
                        selected_limits.fingerprint,
                        _path_sha256(path),
                        witness.device,
                        witness.inode,
                        witness.mode,
                        witness.links,
                    ),
                )
                if _database_size(database) > selected_limits.max_database_bytes:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "candidate catalogue exceeds its database cap"
                    )
                if fault_injector is not None:
                    fault_injector("candidate.create.before_commit")
                database.commit()
                if fault_injector is not None:
                    fault_injector("candidate.create.after_commit")
        except BaseException:
            raise
        barrier(path.parent)
        return cls(
            path,
            expected_store_identity_sha256=store_identity,
            limits=selected_limits,
            clock=clock,
            directory_fsync=directory_fsync,
            fault_injector=fault_injector,
        )

    @property
    def store_identity_sha256(self) -> str:
        self._assert_live()
        return self._expected_store_identity_sha256

    @property
    def store_instance_sha256(self) -> str:
        self._assert_live()
        return self._store_instance_sha256

    @property
    def source_binding_sha256(self) -> str:
        self._assert_live()
        return self._source_binding_sha256

    @property
    def archive_store_identity_sha256(self) -> str:
        self._assert_live()
        return self._archive_store_identity_sha256

    @property
    def authority_sha256(self) -> str:
        self._assert_live()
        return super().authority_sha256

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(
            self.path,
            isolation_level=None,
            timeout=self._limits.busy_timeout_seconds,
        )
        _configure_database(
            database,
            busy_timeout_seconds=self._limits.busy_timeout_seconds,
        )
        return database

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _assert_live(self) -> None:
        _assert_file_witness(
            self.path, self._witness, label="candidate catalogue"
        )

    def _validate(self, database: sqlite3.Connection) -> sqlite3.Row:
        self._assert_live()
        if int(database.execute("PRAGMA user_version").fetchone()[0]) != 1:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue schema version differs"
            )
        check = tuple(database.execute("PRAGMA quick_check(1)").fetchone())
        if check != ("ok",):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue quick_check failed"
            )
        objects = database.execute(
            "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
        table_names = {str(row["name"]) for row in objects if row["type"] == "table"}
        trigger_names = {
            str(row["name"]) for row in objects if row["type"] == "trigger"
        }
        expected_triggers = {
            statement.split()[2] for statement in _CANDIDATE_TRIGGER_DDL
        }
        if table_names != set(_CANDIDATE_COLUMNS) or trigger_names != expected_triggers:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue schema inventory differs"
            )
        actual_trigger_sql = {
            str(row["name"]): _normalize_sql(row["sql"])
            for row in objects if row["type"] == "trigger"
        }
        expected_trigger_sql = {
            statement.split()[2]: _normalize_sql(statement)
            for statement in _CANDIDATE_TRIGGER_DDL
        }
        if actual_trigger_sql != expected_trigger_sql:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue trigger DDL differs"
            )
        for table, expected in _CANDIDATE_COLUMNS.items():
            actual = tuple(
                str(row["name"])
                for row in database.execute(f"PRAGMA table_info({table})")
            )
            if actual != expected:
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "candidate catalogue table shape differs"
                )
        meta = database.execute(
            "SELECT * FROM candidate_catalog_meta WHERE singleton=1"
        ).fetchone()
        if meta is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue metadata is unavailable"
            )
        witness = self._witness
        expected_identity = _digest({
            "archive_store_identity_sha256": str(meta["archive_store_identity_sha256"]),
            "canonical_path_sha256": _path_sha256(self.path),
            "file_witness": witness.as_dict(),
            "limits_sha256": self._limits.fingerprint,
            "schema": CANDIDATE_CATALOG_IDENTITY_SCHEMA,
            "source_binding_sha256": str(meta["source_binding_sha256"]),
            "store_contract": CANDIDATE_CATALOG_CONTRACT,
            "store_instance_sha256": str(meta["store_instance_sha256"]),
        })
        expected_authority = _digest({
            "canonical_path_sha256": _path_sha256(self.path),
            "schema": CANDIDATE_AUTHORITY_SCHEMA,
            "store_identity_sha256": expected_identity,
        })
        if (
            str(meta["store_contract"]) != CANDIDATE_CATALOG_CONTRACT
            or int(meta["schema_version"]) != CANDIDATE_CATALOG_SCHEMA_VERSION
            or str(meta["limits_sha256"]) != self._limits.fingerprint
            or str(meta["canonical_path_sha256"]) != _path_sha256(self.path)
            or int(meta["file_device"]) != witness.device
            or int(meta["file_inode"]) != witness.inode
            or int(meta["file_mode"]) != witness.mode
            or int(meta["file_links"]) != witness.links
            or str(meta["store_identity_sha256"]) != expected_identity
            or str(meta["authority_sha256"]) != expected_authority
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate catalogue authority metadata differs"
            )
        if _database_size(database) > self._limits.max_database_bytes:
            raise AcquisitionSourceReclamationApplyCapacityError(
                "candidate catalogue exceeds its database cap"
            )
        return meta

    @staticmethod
    def _binding_from_blob(value: object) -> SourceReclamationCandidateBindingV1:
        decoded = _decode_canonical(value, label="candidate binding")
        fields = set(SourceReclamationCandidateBindingV1.__dataclass_fields__)
        if set(decoded) != fields | {"schema"}:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate binding shape differs"
            )
        return SourceReclamationCandidateBindingV1(
            **{key: decoded[key] for key in fields}
        )

    @staticmethod
    def _request_from_blob(value: object) -> SourceReclamationRequestV2:
        decoded = _decode_canonical(value, label="candidate request")
        fields = set(SourceReclamationRequestV2.__dataclass_fields__)
        if set(decoded) != fields | {"protocol_contract", "schema"}:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate request shape differs"
            )
        return SourceReclamationRequestV2(**{
            key: decoded[key] for key in fields
        })

    @staticmethod
    def _seal_from_blob(value: object) -> CandidateCatalogSealReceiptV1:
        decoded = _decode_canonical(value, label="candidate seal receipt")
        fields = set(CandidateCatalogSealReceiptV1.__dataclass_fields__)
        if set(decoded) != fields | {"schema"}:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate seal receipt shape differs"
            )
        return CandidateCatalogSealReceiptV1(**{
            key: decoded[key] for key in fields
        })

    @staticmethod
    def _validate_reference_payload(
        payload: bytes,
        binding: SourceReclamationCandidateBindingV1,
    ) -> dict[str, Any]:
        decoded = _decode_canonical(payload, label="candidate metadata reference")
        for role in PARTICIPANT_APPLY_ORDER:
            candidate_participant_view_sha256(decoded, role)
        if (
            decoded.get("candidate_sha256") != binding.candidate_sha256
            or decoded.get("record_sha256")
            != binding.sealed_archive_record_sha256
            or decoded.get("sealed_archive_segment_sha256")
            != binding.sealed_archive_segment_sha256
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "candidate metadata reference differs from its binding"
            )
        return decoded

    def seal_request_v2(
        self,
        request: SourceReclamationRequestV2,
        *,
        bindings: tuple[SourceReclamationCandidateBindingV1, ...],
        record_payloads: tuple[bytes, ...],
    ) -> CandidateCatalogSealReceiptV1:
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        if request.candidate_authority_sha256 != self.authority_sha256:
            raise AcquisitionSourceReclamationApplyConflict(
                "request candidate authority differs"
            )
        if request.source_binding_sha256 != self.source_binding_sha256:
            raise AcquisitionSourceReclamationApplyConflict(
                "request source binding differs"
            )
        if (
            not isinstance(bindings, tuple)
            or not isinstance(record_payloads, tuple)
            or len(bindings) != request.candidate_count
            or len(record_payloads) != len(bindings)
            or len(bindings) > self._limits.max_candidates_per_request
        ):
            raise AcquisitionSourceReclamationApplyCapacityError(
                "candidate seal inventory shape or count differs"
            )
        calculated_candidates = candidate_binding_root(bindings)
        calculated_archive = archive_binding_root(bindings, record_payloads)
        if (
            calculated_candidates != request.candidate_bindings_sha256
            or calculated_archive != request.archive_binding_root_sha256
        ):
            raise AcquisitionSourceReclamationApplyConflict(
                "candidate seal roots differ from request"
            )
        for binding, payload in zip(bindings, record_payloads, strict=True):
            self._validate_reference_payload(payload, binding)
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                existing = database.execute(
                    "SELECT * FROM candidate_requests WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if existing is not None:
                    stored = self._request_from_blob(existing["request_json"])
                    seal = self._seal_from_blob(existing["seal_receipt_json"])
                    rows = database.execute(
                        "SELECT * FROM candidate_records WHERE request_sha256=? "
                        "ORDER BY ordinal",
                        (request.request_sha256,),
                    ).fetchall()
                    if (
                        stored != request
                        or len(rows) != len(bindings)
                        or any(
                            bytes(row["binding_json"])
                            != canonical_bytes(binding.as_dict())
                            or bytes(row["reference_json"]) != payload
                            for row, binding, payload in zip(
                                rows, bindings, record_payloads, strict=True
                            )
                        )
                    ):
                        raise AcquisitionSourceReclamationApplyConflict(
                            "candidate request identity was reused"
                        )
                    database.commit()
                    return seal
                request_count = int(database.execute(
                    "SELECT count(*) FROM candidate_requests"
                ).fetchone()[0])
                record_count = int(database.execute(
                    "SELECT count(*) FROM candidate_records"
                ).fetchone()[0])
                if (
                    request_count >= self._limits.max_requests
                    or record_count + len(bindings) > self._limits.max_total_records
                ):
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "candidate catalogue row cap reached"
                    )
                observed = _finite_timestamp(
                    self._clock(), label="candidate seal clock"
                )
                latest = database.execute(
                    "SELECT max(sealed_at) FROM candidate_requests"
                ).fetchone()[0]
                if latest is not None and observed < float(latest):
                    raise AcquisitionSourceReclamationApplyClockError(
                        "candidate seal clock rollback"
                    )
                seal = CandidateCatalogSealReceiptV1.create(
                    request_sha256=request.request_sha256,
                    candidate_authority_sha256=self.authority_sha256,
                    candidate_bindings_sha256=calculated_candidates,
                    archive_binding_root_sha256=calculated_archive,
                    candidate_count=len(bindings),
                    sealed_at=observed,
                )
                database.execute(
                    "INSERT INTO candidate_requests VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        request.request_sha256,
                        sqlite3.Binary(canonical_bytes(request.as_dict())),
                        calculated_candidates,
                        calculated_archive,
                        request.sealed_archive_anchor_sha256,
                        len(bindings),
                        seal.receipt_sha256,
                        sqlite3.Binary(_canonical(seal.as_dict())),
                        observed,
                    ),
                )
                for ordinal, (binding, payload) in enumerate(
                    zip(bindings, record_payloads, strict=True)
                ):
                    database.execute(
                        "INSERT INTO candidate_records VALUES(?,?,?,?,?,?,?)",
                        (
                            request.request_sha256,
                            ordinal,
                            binding.candidate_sha256,
                            binding.binding_sha256,
                            sqlite3.Binary(canonical_bytes(binding.as_dict())),
                            hashlib.sha256(payload).hexdigest(),
                            sqlite3.Binary(payload),
                        ),
                    )
                if _database_size(database) > self._limits.max_database_bytes:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "candidate catalogue database cap reached"
                    )
                self._fault("candidate.seal.before_commit")
                database.commit()
                self._fault("candidate.seal.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        return seal

    @contextmanager
    def hold_candidate_page_v1(
        self,
        request: SourceReclamationRequestV2,
        *,
        after_cursor: int,
        max_candidates: int,
    ) -> Iterator[SourceReclamationCandidatePageV1]:
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        require_count(after_cursor, "candidate page cursor", maximum=4096)
        require_count(max_candidates, "candidate page size", maximum=64)
        if max_candidates < 1 or after_cursor >= request.candidate_count:
            raise AcquisitionSourceReclamationApplyCapacityError(
                "candidate page request is outside its bound"
            )
        if request.candidate_authority_sha256 != self.authority_sha256:
            raise AcquisitionSourceReclamationApplyConflict(
                "candidate page authority differs"
            )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN")
                self._validate(database)
                request_row = database.execute(
                    "SELECT * FROM candidate_requests WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if request_row is None:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "sealed candidate request is unavailable"
                    )
                stored_request = self._request_from_blob(request_row["request_json"])
                all_rows = database.execute(
                    "SELECT * FROM candidate_records WHERE request_sha256=? "
                    "ORDER BY ordinal",
                    (request.request_sha256,),
                ).fetchall()
                bindings = tuple(
                    self._binding_from_blob(row["binding_json"])
                    for row in all_rows
                )
                payloads = tuple(bytes(row["reference_json"]) for row in all_rows)
                if (
                    stored_request != request
                    or len(all_rows) != request.candidate_count
                    or tuple(int(row["ordinal"]) for row in all_rows)
                    != tuple(range(request.candidate_count))
                    or any(
                        str(row["binding_sha256"]) != binding.binding_sha256
                        or str(row["candidate_sha256"]) != binding.candidate_sha256
                        or str(row["reference_sha256"])
                        != hashlib.sha256(payload).hexdigest()
                        for row, binding, payload in zip(
                            all_rows, bindings, payloads, strict=True
                        )
                    )
                    or candidate_binding_root(bindings)
                    != request.candidate_bindings_sha256
                    or archive_binding_root(bindings, payloads)
                    != request.archive_binding_root_sha256
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "sealed candidate inventory differs"
                    )
                for binding, payload in zip(bindings, payloads, strict=True):
                    self._validate_reference_payload(payload, binding)
                upper = min(request.candidate_count, after_cursor + max_candidates)
                page = SourceReclamationCandidatePageV1.create(
                    request_sha256=request.request_sha256,
                    candidate_authority_sha256=self.authority_sha256,
                    sealed_archive_anchor_sha256=request.sealed_archive_anchor_sha256,
                    archive_binding_root_sha256=request.archive_binding_root_sha256,
                    candidate_bindings_sha256=request.candidate_bindings_sha256,
                    consumer_registry_root_sha256=request.consumer_registry_root_sha256,
                    consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
                    after_cursor=after_cursor,
                    total_candidate_count=request.candidate_count,
                    bindings=bindings[after_cursor:upper],
                    record_payloads=payloads[after_cursor:upper],
                )
                self._fault("candidate.hold.before_yield")
                yield page
                self._assert_live()
                current = database.execute(
                    "SELECT seal_receipt_sha256 FROM candidate_requests "
                    "WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if (
                    current is None
                    or str(current[0]) != str(request_row["seal_receipt_sha256"])
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "held candidate seal changed"
                    )
                self._fault("candidate.hold.after_reproof")
            finally:
                if database.in_transaction:
                    database.rollback()


@dataclass(frozen=True, slots=True)
class AcquisitionSourceReclamationCoordinatorLimits:
    max_requests: int = 4096
    max_events: int = 65_536
    max_receipt_bytes: int = 512 * 1024
    max_database_bytes: int = 128 * 1024 * 1024
    busy_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        require_count(self.max_requests, "coordinator request cap", maximum=2**31 - 1)
        require_count(self.max_events, "coordinator event cap", maximum=2**31 - 1)
        if self.max_requests < 1 or self.max_events < self.max_requests * 2:
            raise ValueError("coordinator row caps are invalid")
        if not 1024 <= self.max_receipt_bytes <= 16 * 1024 * 1024:
            raise ValueError("coordinator receipt byte cap is invalid")
        if not 1024 * 1024 <= self.max_database_bytes <= 16 * 1024**3:
            raise ValueError("coordinator database byte cap is invalid")
        if (
            not math.isfinite(float(self.busy_timeout_seconds))
            or not 0.001 <= float(self.busy_timeout_seconds) <= 60.0
        ):
            raise ValueError("coordinator busy timeout is invalid")

    def as_dict(self) -> dict[str, Any]:
        return {
            "busy_timeout_seconds": float(self.busy_timeout_seconds),
            "max_database_bytes": self.max_database_bytes,
            "max_events": self.max_events,
            "max_receipt_bytes": self.max_receipt_bytes,
            "max_requests": self.max_requests,
            "schema": COORDINATOR_LIMITS_SCHEMA,
        }

    @property
    def fingerprint(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True, slots=True)
class CoordinatorStateSnapshotV2:
    snapshot_sha256: str
    request_sha256: str
    state: str
    owner_epoch: int
    prepared_roles: tuple[str, ...]
    applied_roles: tuple[str, ...]
    aborted_roles: tuple[str, ...]
    committed_receipt_sha256: str | None
    abort_receipt_sha256: str | None
    event_sequence: int
    head_event_sha256: str
    updated_at: float

    def __post_init__(self) -> None:
        require_sha256(self.snapshot_sha256, "coordinator snapshot SHA-256")
        require_sha256(self.request_sha256, "coordinator snapshot request SHA-256")
        if self.state not in {
            PREPARING, PREPARED, COMMITTED, APPLIED, ABORTING, ABORTED
        }:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator snapshot state differs"
            )
        require_count(self.owner_epoch, "coordinator owner epoch", maximum=2**63 - 1)
        require_count(
            self.event_sequence,
            "coordinator snapshot event sequence",
            maximum=2**63 - 1,
        )
        if self.owner_epoch < 1 or self.event_sequence < 1:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator snapshot sequence is invalid"
            )
        require_sha256(self.head_event_sha256, "coordinator head event SHA-256")
        for roles, label in (
            (self.prepared_roles, "prepared"),
            (self.applied_roles, "applied"),
            (self.aborted_roles, "aborted"),
        ):
            if (
                not isinstance(roles, tuple)
                or any(role not in PARTICIPANT_APPLY_ORDER for role in roles)
                or tuple(role for role in PARTICIPANT_APPLY_ORDER if role in roles)
                != roles
            ):
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    f"coordinator {label} role inventory differs"
                )
        if self.applied_roles != PARTICIPANT_APPLY_ORDER[: len(self.applied_roles)]:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator apply progress is not a prefix"
            )
        for value, label in (
            (self.committed_receipt_sha256, "coordinator committed receipt SHA-256"),
            (self.abort_receipt_sha256, "coordinator abort receipt SHA-256"),
        ):
            if value is not None:
                require_sha256(value, label)
        require_timestamp(self.updated_at, "coordinator snapshot clock")
        if self.snapshot_sha256 != _digest(self._core()):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator snapshot checksum differs"
            )

    def _core(self) -> dict[str, Any]:
        return {
            "abort_receipt_sha256": self.abort_receipt_sha256,
            "aborted_roles": list(self.aborted_roles),
            "applied_roles": list(self.applied_roles),
            "committed_receipt_sha256": self.committed_receipt_sha256,
            "event_sequence": self.event_sequence,
            "head_event_sha256": self.head_event_sha256,
            "owner_epoch": self.owner_epoch,
            "prepared_roles": list(self.prepared_roles),
            "request_sha256": self.request_sha256,
            "schema": COORDINATOR_SNAPSHOT_SCHEMA,
            "state": self.state,
            "updated_at": float(self.updated_at),
        }

    def as_dict(self) -> dict[str, Any]:
        return {**self._core(), "snapshot_sha256": self.snapshot_sha256}

    @classmethod
    def create(
        cls,
        *,
        request_sha256: str,
        state: str,
        owner_epoch: int,
        prepared_roles: tuple[str, ...],
        applied_roles: tuple[str, ...],
        aborted_roles: tuple[str, ...],
        committed_receipt_sha256: str | None,
        abort_receipt_sha256: str | None,
        event_sequence: int,
        head_event_sha256: str,
        updated_at: float,
    ) -> "CoordinatorStateSnapshotV2":
        values = {
            "request_sha256": request_sha256,
            "state": state,
            "owner_epoch": owner_epoch,
            "prepared_roles": prepared_roles,
            "applied_roles": applied_roles,
            "aborted_roles": aborted_roles,
            "committed_receipt_sha256": committed_receipt_sha256,
            "abort_receipt_sha256": abort_receipt_sha256,
            "event_sequence": event_sequence,
            "head_event_sha256": head_event_sha256,
            "updated_at": updated_at,
        }
        core = {
            "abort_receipt_sha256": abort_receipt_sha256,
            "aborted_roles": list(aborted_roles),
            "applied_roles": list(applied_roles),
            "committed_receipt_sha256": committed_receipt_sha256,
            "event_sequence": event_sequence,
            "head_event_sha256": head_event_sha256,
            "owner_epoch": owner_epoch,
            "prepared_roles": list(prepared_roles),
            "request_sha256": request_sha256,
            "schema": COORDINATOR_SNAPSHOT_SCHEMA,
            "state": state,
            "updated_at": float(updated_at),
        }
        return cls(snapshot_sha256=_digest(core), **values)


_COORDINATOR_TABLE_DDL = (
    """
    CREATE TABLE coordinator_meta(
        singleton INTEGER PRIMARY KEY CHECK(singleton=1),
        store_contract TEXT NOT NULL,
        schema_version INTEGER NOT NULL,
        store_instance_sha256 TEXT NOT NULL,
        store_identity_sha256 TEXT NOT NULL,
        authority_sha256 TEXT NOT NULL,
        candidate_authority_sha256 TEXT NOT NULL,
        limits_sha256 TEXT NOT NULL,
        canonical_path_sha256 TEXT NOT NULL,
        file_device INTEGER NOT NULL,
        file_inode INTEGER NOT NULL,
        file_mode INTEGER NOT NULL,
        file_links INTEGER NOT NULL,
        next_owner_epoch INTEGER NOT NULL CHECK(next_owner_epoch>=1),
        next_event_sequence INTEGER NOT NULL CHECK(next_event_sequence>=1),
        head_event_sha256 TEXT NOT NULL,
        last_observed_at REAL
    )
    """,
    """
    CREATE TABLE coordinator_requests(
        request_sha256 TEXT PRIMARY KEY,
        request_json BLOB NOT NULL,
        state TEXT NOT NULL CHECK(state IN ('PREPARING','PREPARED','COMMITTED','APPLIED','ABORTING','ABORTED')),
        owner_epoch INTEGER NOT NULL,
        active_slot INTEGER UNIQUE CHECK(active_slot IS NULL OR active_slot=1),
        participant_prepare_root_sha256 TEXT,
        committed_decision_sha256 TEXT,
        committed_receipt_sha256 TEXT,
        committed_receipt_json BLOB,
        abort_decision_sha256 TEXT,
        abort_receipt_sha256 TEXT,
        abort_receipt_json BLOB,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        completed_at REAL
    )
    """,
    """
    CREATE TABLE coordinator_prepares(
        request_sha256 TEXT NOT NULL,
        role TEXT NOT NULL,
        source_store_identity_sha256 TEXT NOT NULL,
        prepare_receipt_sha256 TEXT NOT NULL,
        source_fence_sha256 TEXT NOT NULL,
        receipt_json BLOB NOT NULL,
        recorded_at REAL NOT NULL,
        PRIMARY KEY(request_sha256,role),
        FOREIGN KEY(request_sha256) REFERENCES coordinator_requests(request_sha256)
    )
    """,
    """
    CREATE TABLE coordinator_applies(
        request_sha256 TEXT NOT NULL,
        role TEXT NOT NULL,
        apply_receipt_sha256 TEXT NOT NULL,
        predecessor_apply_receipt_sha256 TEXT,
        receipt_json BLOB NOT NULL,
        recorded_at REAL NOT NULL,
        PRIMARY KEY(request_sha256,role),
        FOREIGN KEY(request_sha256) REFERENCES coordinator_requests(request_sha256)
    )
    """,
    """
    CREATE TABLE coordinator_aborts(
        request_sha256 TEXT NOT NULL,
        role TEXT NOT NULL,
        abort_receipt_sha256 TEXT NOT NULL,
        receipt_json BLOB NOT NULL,
        recorded_at REAL NOT NULL,
        PRIMARY KEY(request_sha256,role),
        FOREIGN KEY(request_sha256) REFERENCES coordinator_requests(request_sha256)
    )
    """,
    """
    CREATE TABLE coordinator_events(
        event_sequence INTEGER PRIMARY KEY,
        event_sha256 TEXT NOT NULL UNIQUE,
        previous_event_sha256 TEXT NOT NULL,
        request_sha256 TEXT NOT NULL,
        event_type TEXT NOT NULL,
        state TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        owner_epoch INTEGER NOT NULL,
        observed_at REAL NOT NULL,
        FOREIGN KEY(request_sha256) REFERENCES coordinator_requests(request_sha256)
    )
    """,
)

_COORDINATOR_TRIGGER_DDL = (
    "CREATE TRIGGER coordinator_meta_no_delete BEFORE DELETE ON coordinator_meta BEGIN SELECT RAISE(ABORT,'coordinator_meta_no_delete'); END",
    """CREATE TRIGGER coordinator_meta_guard BEFORE UPDATE ON coordinator_meta
    WHEN NEW.store_contract!=OLD.store_contract OR NEW.schema_version!=OLD.schema_version
      OR NEW.store_instance_sha256!=OLD.store_instance_sha256
      OR NEW.store_identity_sha256!=OLD.store_identity_sha256
      OR NEW.authority_sha256!=OLD.authority_sha256
      OR NEW.candidate_authority_sha256!=OLD.candidate_authority_sha256
      OR NEW.limits_sha256!=OLD.limits_sha256
      OR NEW.canonical_path_sha256!=OLD.canonical_path_sha256
      OR NEW.file_device!=OLD.file_device OR NEW.file_inode!=OLD.file_inode
      OR NEW.file_mode!=OLD.file_mode OR NEW.file_links!=OLD.file_links
      OR NEW.next_owner_epoch<OLD.next_owner_epoch
      OR NEW.next_event_sequence<OLD.next_event_sequence
      OR (OLD.last_observed_at IS NOT NULL AND NEW.last_observed_at<OLD.last_observed_at)
    BEGIN SELECT RAISE(ABORT,'coordinator_meta_guard'); END""",
    "CREATE TRIGGER coordinator_requests_no_delete BEFORE DELETE ON coordinator_requests BEGIN SELECT RAISE(ABORT,'coordinator_request_no_delete'); END",
    """CREATE TRIGGER coordinator_requests_guard BEFORE UPDATE ON coordinator_requests
    WHEN NEW.request_sha256!=OLD.request_sha256 OR NEW.request_json!=OLD.request_json
      OR NEW.owner_epoch!=OLD.owner_epoch OR NEW.created_at!=OLD.created_at
      OR NOT ((OLD.state='PREPARING' AND NEW.state IN ('PREPARING','PREPARED','ABORTING'))
        OR (OLD.state='PREPARED' AND NEW.state IN ('PREPARED','COMMITTED','ABORTING'))
        OR (OLD.state='COMMITTED' AND NEW.state IN ('COMMITTED','APPLIED'))
        OR (OLD.state='APPLIED' AND NEW.state='APPLIED')
        OR (OLD.state='ABORTING' AND NEW.state IN ('ABORTING','ABORTED'))
        OR (OLD.state='ABORTED' AND NEW.state='ABORTED'))
    BEGIN SELECT RAISE(ABORT,'coordinator_request_guard'); END""",
    "CREATE TRIGGER coordinator_prepares_no_update BEFORE UPDATE ON coordinator_prepares BEGIN SELECT RAISE(ABORT,'coordinator_prepare_immutable'); END",
    "CREATE TRIGGER coordinator_prepares_no_delete BEFORE DELETE ON coordinator_prepares BEGIN SELECT RAISE(ABORT,'coordinator_prepare_immutable'); END",
    "CREATE TRIGGER coordinator_applies_no_update BEFORE UPDATE ON coordinator_applies BEGIN SELECT RAISE(ABORT,'coordinator_apply_immutable'); END",
    "CREATE TRIGGER coordinator_applies_no_delete BEFORE DELETE ON coordinator_applies BEGIN SELECT RAISE(ABORT,'coordinator_apply_immutable'); END",
    "CREATE TRIGGER coordinator_aborts_no_update BEFORE UPDATE ON coordinator_aborts BEGIN SELECT RAISE(ABORT,'coordinator_abort_immutable'); END",
    "CREATE TRIGGER coordinator_aborts_no_delete BEFORE DELETE ON coordinator_aborts BEGIN SELECT RAISE(ABORT,'coordinator_abort_immutable'); END",
    "CREATE TRIGGER coordinator_events_no_update BEFORE UPDATE ON coordinator_events BEGIN SELECT RAISE(ABORT,'coordinator_event_immutable'); END",
    "CREATE TRIGGER coordinator_events_no_delete BEFORE DELETE ON coordinator_events BEGIN SELECT RAISE(ABORT,'coordinator_event_immutable'); END",
)

_COORDINATOR_COLUMNS = {
    "coordinator_meta": (
        "singleton", "store_contract", "schema_version", "store_instance_sha256",
        "store_identity_sha256", "authority_sha256", "candidate_authority_sha256",
        "limits_sha256", "canonical_path_sha256", "file_device", "file_inode",
        "file_mode", "file_links", "next_owner_epoch", "next_event_sequence",
        "head_event_sha256", "last_observed_at",
    ),
    "coordinator_requests": (
        "request_sha256", "request_json", "state", "owner_epoch", "active_slot",
        "participant_prepare_root_sha256", "committed_decision_sha256",
        "committed_receipt_sha256", "committed_receipt_json",
        "abort_decision_sha256", "abort_receipt_sha256", "abort_receipt_json",
        "created_at", "updated_at", "completed_at",
    ),
    "coordinator_prepares": (
        "request_sha256", "role", "source_store_identity_sha256",
        "prepare_receipt_sha256", "source_fence_sha256", "receipt_json", "recorded_at",
    ),
    "coordinator_applies": (
        "request_sha256", "role", "apply_receipt_sha256",
        "predecessor_apply_receipt_sha256", "receipt_json", "recorded_at",
    ),
    "coordinator_aborts": (
        "request_sha256", "role", "abort_receipt_sha256", "receipt_json", "recorded_at",
    ),
    "coordinator_events": (
        "event_sequence", "event_sha256", "previous_event_sha256", "request_sha256",
        "event_type", "state", "payload_sha256", "owner_epoch", "observed_at",
    ),
}


def _request_from_blob(value: object) -> SourceReclamationRequestV2:
    decoded = _decode_canonical(value, label="coordinator request")
    fields = set(SourceReclamationRequestV2.__dataclass_fields__)
    if set(decoded) != fields | {"protocol_contract", "schema"}:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator request shape differs"
        )
    return SourceReclamationRequestV2(**{key: decoded[key] for key in fields})


def _prepare_from_blob(value: object) -> SourceLocalPrepareReceiptV2:
    decoded = _decode_canonical(value, label="coordinator prepare receipt")
    fields = set(SourceLocalPrepareReceiptV2.__dataclass_fields__)
    if set(decoded) != fields | {"protocol_contract", "schema"}:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator prepare receipt shape differs"
        )
    return SourceLocalPrepareReceiptV2(**{key: decoded[key] for key in fields})


def _apply_from_blob(value: object) -> SourceLocalApplyReceiptV1:
    decoded = _decode_canonical(value, label="coordinator apply receipt")
    fields = set(SourceLocalApplyReceiptV1.__dataclass_fields__)
    if set(decoded) != fields | {"protocol_contract", "schema"}:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator apply receipt shape differs"
        )
    return SourceLocalApplyReceiptV1(**{key: decoded[key] for key in fields})


def _abort_from_blob(value: object) -> SourceLocalAbortReceiptV1:
    decoded = _decode_canonical(value, label="coordinator abort receipt")
    fields = set(SourceLocalAbortReceiptV1.__dataclass_fields__)
    if set(decoded) != fields | {"protocol_contract", "schema"}:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator local abort receipt shape differs"
        )
    return SourceLocalAbortReceiptV1(**{key: decoded[key] for key in fields})


def _committed_from_blob(value: object) -> CommittedReclamationDecisionReceiptV1:
    decoded = _decode_canonical(value, label="coordinator committed receipt")
    fields = set(CommittedReclamationDecisionReceiptV1.__dataclass_fields__)
    expected = fields | {"apply_order", "protocol_contract", "schema"}
    if set(decoded) != expected or decoded.get("apply_order") != list(
        PARTICIPANT_APPLY_ORDER
    ):
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator committed receipt shape differs"
        )
    prepares_value = decoded["participant_prepares"]
    if not isinstance(prepares_value, list):
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator committed prepare inventory differs"
        )
    prepares = tuple(
        ParticipantPrepareBindingV1(**{
            key: item[key]
            for key in ParticipantPrepareBindingV1.__dataclass_fields__
        })
        for item in prepares_value
        if isinstance(item, dict)
        and set(item) == set(ParticipantPrepareBindingV1.__dataclass_fields__) | {"schema"}
    )
    if len(prepares) != len(prepares_value):
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator committed prepare item differs"
        )
    values = {key: decoded[key] for key in fields if key != "participant_prepares"}
    return CommittedReclamationDecisionReceiptV1(
        participant_prepares=prepares,
        **values,
    )


def _coordinator_abort_from_blob(
    value: object,
) -> CoordinatorAbortDecisionReceiptV1:
    decoded = _decode_canonical(value, label="coordinator abort decision")
    fields = set(CoordinatorAbortDecisionReceiptV1.__dataclass_fields__)
    if set(decoded) != fields | {"protocol_contract", "schema"}:
        raise AcquisitionSourceReclamationApplyIntegrityError(
            "coordinator abort decision shape differs"
        )
    return CoordinatorAbortDecisionReceiptV1(**{
        key: decoded[key] for key in fields
    })


class SqliteHeldCommittedDecisionAuthority(HeldCommittedDecisionAuthority):
    """Concrete physical decision/progress authority for one coordinator."""

    def __init__(
        self,
        coordinator: "AcquisitionSourceReclamationApplyCoordinator",
    ) -> None:
        if not isinstance(coordinator, AcquisitionSourceReclamationApplyCoordinator):
            raise TypeError("coordinator must be an apply coordinator")
        self._coordinator = coordinator
        super().__init__(coordinator.authority_sha256)

    @contextmanager
    def hold_committed_decision_v1(
        self,
        request_sha256: str,
        *,
        applying_role: str,
        expected_predecessor_apply_receipt_sha256: str | None,
    ) -> Iterator[CommittedReclamationDecisionReceiptV1]:
        request = require_sha256(request_sha256, "held commit request SHA-256")
        role = require_role(applying_role)
        role_index = PARTICIPANT_APPLY_ORDER.index(role)
        if role_index == 0:
            if expected_predecessor_apply_receipt_sha256 is not None:
                raise AcquisitionSourceReclamationApplyConflict(
                    "first apply role unexpectedly has a predecessor"
                )
        else:
            require_sha256(
                expected_predecessor_apply_receipt_sha256,
                "held predecessor apply SHA-256",
            )
        owner = self._coordinator
        with owner._lock, closing(owner._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                owner._validate(database)
                receipt, progress = owner._held_apply_authority_state(
                    database,
                    request,
                    applying_role=role,
                    expected_predecessor_apply_receipt_sha256=(
                        expected_predecessor_apply_receipt_sha256
                    ),
                )
                durable_bytes = owner._held_decision_durable_bytes(
                    database, request
                )
                owner._fault("decision_hold.commit.before_yield")
                yield receipt
                owner._assert_live()
                owner._validate(database)
                repeated, repeated_progress = owner._held_apply_authority_state(
                    database,
                    request,
                    applying_role=role,
                    expected_predecessor_apply_receipt_sha256=(
                        expected_predecessor_apply_receipt_sha256
                    ),
                )
                repeated_bytes = owner._held_decision_durable_bytes(
                    database, request
                )
                if (
                    repeated != receipt
                    or repeated_progress != progress
                    or repeated_bytes != durable_bytes
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "held coordinator decision/progress bytes changed"
                    )
                owner._fault("decision_hold.commit.after_reproof")
            finally:
                if database.in_transaction:
                    database.rollback()

    @contextmanager
    def hold_abort_decision_v1(
        self,
        request_sha256: str,
        *,
        expected_abort_decision_sha256: str,
        aborting_role: str,
        expected_prepare_receipt_sha256: str,
        expected_source_store_identity_sha256: str,
        expected_source_fence_sha256: str,
    ) -> Iterator[CoordinatorAbortDecisionReceiptV1]:
        request = require_sha256(request_sha256, "held abort request SHA-256")
        expected_abort = require_sha256(
            expected_abort_decision_sha256,
            "held abort decision SHA-256",
        )
        role = require_role(aborting_role)
        expected_prepare = require_sha256(
            expected_prepare_receipt_sha256,
            "held abort prepare receipt SHA-256",
        )
        expected_store = require_sha256(
            expected_source_store_identity_sha256,
            "held abort store identity SHA-256",
        )
        expected_fence = require_sha256(
            expected_source_fence_sha256,
            "held abort source fence SHA-256",
        )
        owner = self._coordinator
        with owner._lock, closing(owner._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                owner._validate(database)
                receipt, binding = owner._held_aborting_state(
                    database, request, role
                )
                if (
                    receipt.abort_decision_sha256 != expected_abort
                    or binding.prepare_receipt_sha256 != expected_prepare
                    or binding.source_store_identity_sha256 != expected_store
                    or binding.source_fence_sha256 != expected_fence
                ):
                    raise AcquisitionSourceReclamationApplyConflict(
                        "held abort prepare binding differs"
                    )
                owner._fault("decision_hold.abort.before_yield")
                yield receipt
                owner._assert_live()
                repeated, repeated_binding = owner._held_aborting_state(
                    database, request, role
                )
                if repeated != receipt or repeated_binding != binding:
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "held abort decision/binding changed"
                    )
                owner._fault("decision_hold.abort.after_reproof")
            finally:
                if database.in_transaction:
                    database.rollback()


def _candidate_authority_path_overlaps(
    coordinator_path: Path,
    candidate_authority: SourceReclamationCandidateAuthority,
) -> bool:
    """Keep local candidate authorities physically disjoint from the journal.

    The protocol deliberately also permits non-filesystem authorities.  The
    built-in SQLite catalogue exposes ``path``; retention-v2 exposes a whole
    ``root`` plus ``authority_path``.  Unknown protocol implementations have
    no local path to compare and are instead protected by their held page
    authority and immutable authority SHA-256.
    """

    for attribute in ("path", "authority_path"):
        value = getattr(candidate_authority, attribute, None)
        if value is None:
            continue
        authority_path = _canonical_path(
            value,
            label=f"candidate authority {attribute}",
        )
        if coordinator_path == authority_path:
            return True
    root = getattr(candidate_authority, "root", None)
    if root is not None:
        authority_root = _canonical_path(
            root,
            label="candidate authority root",
        )
        if coordinator_path == authority_root or authority_root in (
            coordinator_path.parents
        ):
            return True
    return False


class AcquisitionSourceReclamationApplyCoordinator:
    """Crash-durable inert v2 prepare/decision/progress coordinator."""

    def __init__(
        self,
        database_path: Path | str,
        *,
        expected_store_identity_sha256: str,
        candidate_authority: SourceReclamationCandidateAuthority,
        limits: AcquisitionSourceReclamationCoordinatorLimits | None = None,
        clock: Callable[[], float] = time.time,
        directory_fsync: Callable[[Path], None] | None = None,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = _canonical_path(database_path, label="coordinator database path")
        if not isinstance(
            candidate_authority, SourceReclamationCandidateAuthority
        ):
            raise TypeError(
                "candidate_authority must implement SourceReclamationCandidateAuthority"
            )
        if _candidate_authority_path_overlaps(self.path, candidate_authority):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator and candidate authority paths overlap"
            )
        self._candidate_authority = candidate_authority
        self._candidate_authority_sha256 = candidate_authority.authority_sha256
        self._expected_store_identity_sha256 = require_sha256(
            expected_store_identity_sha256,
            "coordinator expected store identity SHA-256",
        )
        self._limits = limits or AcquisitionSourceReclamationCoordinatorLimits()
        if not callable(clock):
            raise TypeError("coordinator clock must be callable")
        if directory_fsync is not None and not callable(directory_fsync):
            raise TypeError("coordinator directory_fsync must be callable")
        if fault_injector is not None and not callable(fault_injector):
            raise TypeError("coordinator fault injector must be callable")
        self._clock = clock
        self._directory_fsync = directory_fsync or _default_directory_fsync
        self._fault_injector = fault_injector
        self._lock = threading.RLock()
        self._witness = _file_witness(self.path, label="coordinator database")
        with closing(self._connect()) as database:
            meta = self._validate(database)
        if not hmac.compare_digest(
            str(meta["store_identity_sha256"]),
            self._expected_store_identity_sha256,
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator store identity differs"
            )
        self._store_instance_sha256 = require_sha256(
            str(meta["store_instance_sha256"]),
            "coordinator store instance SHA-256",
        )
        self._authority_sha256 = require_sha256(
            str(meta["authority_sha256"]),
            "coordinator authority SHA-256",
        )
        self._decision_authority = SqliteHeldCommittedDecisionAuthority(self)

    @classmethod
    def create(
        cls,
        database_path: Path | str,
        *,
        candidate_authority: SourceReclamationCandidateAuthority,
        limits: AcquisitionSourceReclamationCoordinatorLimits | None = None,
        clock: Callable[[], float] = time.time,
        directory_fsync: Callable[[Path], None] | None = None,
        fault_injector: Callable[[str], None] | None = None,
    ) -> "AcquisitionSourceReclamationApplyCoordinator":
        path = _canonical_path(database_path, label="coordinator database path")
        if not isinstance(
            candidate_authority, SourceReclamationCandidateAuthority
        ):
            raise TypeError(
                "candidate_authority must implement SourceReclamationCandidateAuthority"
            )
        if _candidate_authority_path_overlaps(path, candidate_authority):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator and candidate authority paths overlap"
            )
        selected_limits = limits or AcquisitionSourceReclamationCoordinatorLimits()
        barrier = directory_fsync or _default_directory_fsync
        if path.exists() or path.is_symlink():
            raise AcquisitionSourceReclamationApplyConflict(
                "coordinator database already exists"
            )
        if not path.parent.exists() or not path.parent.is_dir():
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator parent is unavailable"
            )
        database = sqlite3.connect(
            path,
            isolation_level=None,
            timeout=selected_limits.busy_timeout_seconds,
        )
        with closing(database):
            _configure_database(
                database,
                busy_timeout_seconds=selected_limits.busy_timeout_seconds,
            )
            database.execute("PRAGMA user_version=2")
            try:
                database.execute("BEGIN IMMEDIATE")
                for statement in _COORDINATOR_TABLE_DDL:
                    database.execute(statement)
                for statement in _COORDINATOR_TRIGGER_DDL:
                    database.execute(statement)
                witness = _file_witness(path, label="coordinator database")
                instance = secrets.token_hex(32)
                identity = _digest({
                    "candidate_authority_sha256": candidate_authority.authority_sha256,
                    "canonical_path_sha256": _path_sha256(path),
                    "file_witness": witness.as_dict(),
                    "limits_sha256": selected_limits.fingerprint,
                    "schema": COORDINATOR_STORE_IDENTITY_SCHEMA,
                    "store_contract": COORDINATOR_STORE_CONTRACT,
                    "store_instance_sha256": instance,
                })
                authority = _digest({
                    "candidate_authority_sha256": candidate_authority.authority_sha256,
                    "canonical_path_sha256": _path_sha256(path),
                    "schema": COORDINATOR_AUTHORITY_SCHEMA,
                    "store_identity_sha256": identity,
                })
                database.execute(
                    "INSERT INTO coordinator_meta VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
                    (
                        COORDINATOR_STORE_CONTRACT,
                        COORDINATOR_SCHEMA_VERSION,
                        instance,
                        identity,
                        authority,
                        candidate_authority.authority_sha256,
                        selected_limits.fingerprint,
                        _path_sha256(path),
                        witness.device,
                        witness.inode,
                        witness.mode,
                        witness.links,
                        1,
                        1,
                        _ZERO_SHA256,
                    ),
                )
                if _database_size(database) > selected_limits.max_database_bytes:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "coordinator database cap reached during create"
                    )
                if fault_injector is not None:
                    fault_injector("coordinator.create.before_commit")
                database.commit()
                if fault_injector is not None:
                    fault_injector("coordinator.create.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        barrier(path.parent)
        return cls(
            path,
            expected_store_identity_sha256=identity,
            candidate_authority=candidate_authority,
            limits=selected_limits,
            clock=clock,
            directory_fsync=directory_fsync,
            fault_injector=fault_injector,
        )

    @property
    def store_identity_sha256(self) -> str:
        self._assert_live()
        return self._expected_store_identity_sha256

    @property
    def store_instance_sha256(self) -> str:
        self._assert_live()
        return self._store_instance_sha256

    @property
    def authority_sha256(self) -> str:
        self._assert_live()
        return self._authority_sha256

    @property
    def candidate_authority_sha256(self) -> str:
        current = self._candidate_authority.authority_sha256
        if not hmac.compare_digest(current, self._candidate_authority_sha256):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator candidate authority changed"
            )
        return current

    @property
    def decision_authority(self) -> SqliteHeldCommittedDecisionAuthority:
        if not hmac.compare_digest(
            self._decision_authority.authority_sha256, self.authority_sha256
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator decision authority changed"
            )
        return self._decision_authority

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(
            self.path,
            isolation_level=None,
            timeout=self._limits.busy_timeout_seconds,
        )
        _configure_database(
            database,
            busy_timeout_seconds=self._limits.busy_timeout_seconds,
        )
        return database

    def _fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)

    def _assert_live(self) -> None:
        _assert_file_witness(self.path, self._witness, label="coordinator database")

    def _validate(self, database: sqlite3.Connection) -> sqlite3.Row:
        self._assert_live()
        if int(database.execute("PRAGMA user_version").fetchone()[0]) != 2:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator schema version differs"
            )
        check = tuple(database.execute("PRAGMA quick_check(1)").fetchone())
        if check != ("ok",):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator quick_check failed"
            )
        objects = database.execute(
            "SELECT type,name,sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
        ).fetchall()
        table_names = {str(row["name"]) for row in objects if row["type"] == "table"}
        trigger_names = {
            str(row["name"]) for row in objects if row["type"] == "trigger"
        }
        expected_triggers = {
            statement.split()[2] for statement in _COORDINATOR_TRIGGER_DDL
        }
        if table_names != set(_COORDINATOR_COLUMNS) or trigger_names != expected_triggers:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator schema inventory differs"
            )
        actual_trigger_sql = {
            str(row["name"]): _normalize_sql(row["sql"])
            for row in objects if row["type"] == "trigger"
        }
        expected_trigger_sql = {
            statement.split()[2]: _normalize_sql(statement)
            for statement in _COORDINATOR_TRIGGER_DDL
        }
        if actual_trigger_sql != expected_trigger_sql:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator trigger DDL differs"
            )
        for table, expected in _COORDINATOR_COLUMNS.items():
            actual = tuple(
                str(row["name"])
                for row in database.execute(f"PRAGMA table_info({table})")
            )
            if actual != expected:
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator table shape differs"
                )
        meta = database.execute(
            "SELECT * FROM coordinator_meta WHERE singleton=1"
        ).fetchone()
        if meta is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator metadata is unavailable"
            )
        witness = self._witness
        identity = _digest({
            "candidate_authority_sha256": str(meta["candidate_authority_sha256"]),
            "canonical_path_sha256": _path_sha256(self.path),
            "file_witness": witness.as_dict(),
            "limits_sha256": self._limits.fingerprint,
            "schema": COORDINATOR_STORE_IDENTITY_SCHEMA,
            "store_contract": COORDINATOR_STORE_CONTRACT,
            "store_instance_sha256": str(meta["store_instance_sha256"]),
        })
        authority = _digest({
            "candidate_authority_sha256": str(meta["candidate_authority_sha256"]),
            "canonical_path_sha256": _path_sha256(self.path),
            "schema": COORDINATOR_AUTHORITY_SCHEMA,
            "store_identity_sha256": identity,
        })
        if (
            str(meta["store_contract"]) != COORDINATOR_STORE_CONTRACT
            or int(meta["schema_version"]) != COORDINATOR_SCHEMA_VERSION
            or str(meta["candidate_authority_sha256"])
            != self._candidate_authority_sha256
            or str(meta["limits_sha256"]) != self._limits.fingerprint
            or str(meta["canonical_path_sha256"]) != _path_sha256(self.path)
            or int(meta["file_device"]) != witness.device
            or int(meta["file_inode"]) != witness.inode
            or int(meta["file_mode"]) != witness.mode
            or int(meta["file_links"]) != witness.links
            or str(meta["store_identity_sha256"]) != identity
            or str(meta["authority_sha256"]) != authority
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator authority metadata differs"
            )
        request_count = int(database.execute(
            "SELECT count(*) FROM coordinator_requests"
        ).fetchone()[0])
        event_count = int(database.execute(
            "SELECT count(*) FROM coordinator_events"
        ).fetchone()[0])
        if (
            request_count > self._limits.max_requests
            or event_count > self._limits.max_events
            or _database_size(database) > self._limits.max_database_bytes
        ):
            raise AcquisitionSourceReclamationApplyCapacityError(
                "coordinator durable cap exceeded"
            )
        next_sequence = int(meta["next_event_sequence"])
        head = str(meta["head_event_sha256"])
        last_event = database.execute(
            "SELECT event_sequence,event_sha256 FROM coordinator_events "
            "ORDER BY event_sequence DESC LIMIT 1"
        ).fetchone()
        if (
            (last_event is None and (next_sequence != 1 or head != _ZERO_SHA256))
            or (
                last_event is not None
                and (
                    int(last_event["event_sequence"]) != next_sequence - 1
                    or str(last_event["event_sha256"]) != head
                    or event_count != next_sequence - 1
                )
            )
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator event head differs"
            )
        active = database.execute(
            "SELECT state FROM coordinator_requests WHERE active_slot=1"
        ).fetchall()
        if len(active) > 1 or any(
            str(row["state"]) not in {PREPARING, PREPARED, COMMITTED, ABORTING}
            for row in active
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator active request differs"
            )
        return meta

    def _observed(self, database: sqlite3.Connection) -> float:
        observed = _finite_timestamp(self._clock(), label="coordinator clock")
        row = database.execute(
            "SELECT last_observed_at FROM coordinator_meta WHERE singleton=1"
        ).fetchone()
        if row is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator clock authority is unavailable"
            )
        if row[0] is not None and observed < float(row[0]):
            raise AcquisitionSourceReclamationApplyClockError(
                "coordinator clock rollback"
            )
        return observed

    @staticmethod
    def _event_sha256(
        *,
        event_sequence: int,
        previous_event_sha256: str,
        request_sha256: str,
        event_type: str,
        state: str,
        payload_sha256: str,
        owner_epoch: int,
        observed_at: float,
    ) -> str:
        return _digest({
            "event_sequence": event_sequence,
            "event_type": require_identifier(event_type, "coordinator event type"),
            "observed_at": float(observed_at),
            "owner_epoch": owner_epoch,
            "payload_sha256": require_sha256(
                payload_sha256, "coordinator event payload SHA-256"
            ),
            "previous_event_sha256": require_sha256(
                previous_event_sha256, "coordinator previous event SHA-256"
            ),
            "request_sha256": require_sha256(
                request_sha256, "coordinator event request SHA-256"
            ),
            "schema": COORDINATOR_EVENT_SCHEMA,
            "state": state,
        })

    def _event_context(
        self,
        database: sqlite3.Connection,
        *,
        request_sha256: str,
        event_type: str,
        state: str,
        payload_sha256: str,
        owner_epoch: int,
    ) -> tuple[int, str, str, float]:
        meta = database.execute(
            "SELECT * FROM coordinator_meta WHERE singleton=1"
        ).fetchone()
        if meta is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator event metadata is unavailable"
            )
        sequence = int(meta["next_event_sequence"])
        if sequence > self._limits.max_events:
            raise AcquisitionSourceReclamationApplyCapacityError(
                "coordinator event cap reached"
            )
        previous = str(meta["head_event_sha256"])
        observed = self._observed(database)
        event_sha = self._event_sha256(
            event_sequence=sequence,
            previous_event_sha256=previous,
            request_sha256=request_sha256,
            event_type=event_type,
            state=state,
            payload_sha256=payload_sha256,
            owner_epoch=owner_epoch,
            observed_at=observed,
        )
        return sequence, previous, event_sha, observed

    def _write_event(
        self,
        database: sqlite3.Connection,
        *,
        event_sequence: int,
        event_sha256: str,
        previous_event_sha256: str,
        request_sha256: str,
        event_type: str,
        state: str,
        payload_sha256: str,
        owner_epoch: int,
        observed_at: float,
    ) -> None:
        expected = self._event_sha256(
            event_sequence=event_sequence,
            previous_event_sha256=previous_event_sha256,
            request_sha256=request_sha256,
            event_type=event_type,
            state=state,
            payload_sha256=payload_sha256,
            owner_epoch=owner_epoch,
            observed_at=observed_at,
        )
        if expected != event_sha256:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator event checksum differs before append"
            )
        database.execute(
            "INSERT INTO coordinator_events VALUES(?,?,?,?,?,?,?,?,?)",
            (
                event_sequence,
                event_sha256,
                previous_event_sha256,
                request_sha256,
                event_type,
                state,
                payload_sha256,
                owner_epoch,
                observed_at,
            ),
        )
        database.execute(
            "UPDATE coordinator_meta SET next_event_sequence=?,"
            "head_event_sha256=?,last_observed_at=? WHERE singleton=1",
            (event_sequence + 1, event_sha256, observed_at),
        )

    @staticmethod
    def _prepare_bindings(
        receipts: tuple[SourceLocalPrepareReceiptV2, ...],
    ) -> tuple[ParticipantPrepareBindingV1, ...]:
        return tuple(
            ParticipantPrepareBindingV1(
                role=receipt.role,
                prepare_receipt_sha256=receipt.prepare_receipt_sha256,
                source_store_identity_sha256=(
                    receipt.source_store_identity_sha256
                ),
                source_fence_sha256=receipt.source_fence_sha256,
            )
            for receipt in receipts
        )

    @staticmethod
    def _request_row(
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> sqlite3.Row:
        row = database.execute(
            "SELECT * FROM coordinator_requests WHERE request_sha256=?",
            (request_sha256,),
        ).fetchone()
        if row is None:
            raise AcquisitionSourceReclamationApplyConflict(
                "coordinator request is unavailable"
            )
        request = _request_from_blob(row["request_json"])
        if request.request_sha256 != request_sha256:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator request index differs"
            )
        return row

    @staticmethod
    def _prepare_receipts(
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[SourceLocalPrepareReceiptV2, ...]:
        rows = database.execute(
            "SELECT * FROM coordinator_prepares WHERE request_sha256=?",
            (request_sha256,),
        ).fetchall()
        by_role = {str(row["role"]): row for row in rows}
        if (
            len(by_role) != len(rows)
            or not set(by_role).issubset(PARTICIPANT_APPLY_ORDER)
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator prepare roles differ"
            )
        receipts: list[SourceLocalPrepareReceiptV2] = []
        for role in PARTICIPANT_APPLY_ORDER:
            row = by_role.get(role)
            if row is None:
                continue
            receipt = _prepare_from_blob(row["receipt_json"])
            if (
                receipt.request_sha256 != request_sha256
                or receipt.role != role
                or str(row["prepare_receipt_sha256"])
                != receipt.prepare_receipt_sha256
                or str(row["source_store_identity_sha256"])
                != receipt.source_store_identity_sha256
                or str(row["source_fence_sha256"])
                != receipt.source_fence_sha256
            ):
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator prepare row differs"
                )
            receipts.append(receipt)
        return tuple(receipts)

    @staticmethod
    def _apply_receipts(
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[SourceLocalApplyReceiptV1, ...]:
        rows = database.execute(
            "SELECT * FROM coordinator_applies WHERE request_sha256=?",
            (request_sha256,),
        ).fetchall()
        by_role = {str(row["role"]): row for row in rows}
        if (
            len(by_role) != len(rows)
            or not set(by_role).issubset(PARTICIPANT_APPLY_ORDER)
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator apply roles differ"
            )
        receipts: list[SourceLocalApplyReceiptV1] = []
        for role in PARTICIPANT_APPLY_ORDER:
            row = by_role.get(role)
            if row is None:
                continue
            receipt = _apply_from_blob(row["receipt_json"])
            if (
                receipt.request_sha256 != request_sha256
                or receipt.role != role
                or str(row["apply_receipt_sha256"])
                != receipt.apply_receipt_sha256
                or row["predecessor_apply_receipt_sha256"]
                != receipt.predecessor_apply_receipt_sha256
            ):
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator apply row differs"
                )
            receipts.append(receipt)
        result = tuple(receipts)
        if tuple(item.role for item in result) != PARTICIPANT_APPLY_ORDER[: len(result)]:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator apply progress is not a prefix"
            )
        for index, receipt in enumerate(result):
            expected = None if index == 0 else result[index - 1].apply_receipt_sha256
            if receipt.predecessor_apply_receipt_sha256 != expected:
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator apply predecessor chain differs"
                )
        return result

    @staticmethod
    def _abort_receipts(
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[SourceLocalAbortReceiptV1, ...]:
        rows = database.execute(
            "SELECT * FROM coordinator_aborts WHERE request_sha256=?",
            (request_sha256,),
        ).fetchall()
        by_role = {str(row["role"]): row for row in rows}
        if (
            len(by_role) != len(rows)
            or not set(by_role).issubset(PARTICIPANT_APPLY_ORDER)
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator abort roles differ"
            )
        receipts: list[SourceLocalAbortReceiptV1] = []
        for role in PARTICIPANT_APPLY_ORDER:
            row = by_role.get(role)
            if row is None:
                continue
            receipt = _abort_from_blob(row["receipt_json"])
            if (
                receipt.request_sha256 != request_sha256
                or receipt.role != role
                or str(row["abort_receipt_sha256"])
                != receipt.abort_receipt_sha256
            ):
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator abort row differs"
                )
            receipts.append(receipt)
        return tuple(receipts)

    def _snapshot(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> CoordinatorStateSnapshotV2:
        row = self._request_row(database, request_sha256)
        meta = database.execute(
            "SELECT * FROM coordinator_meta WHERE singleton=1"
        ).fetchone()
        if meta is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator snapshot metadata is unavailable"
            )
        prepares = self._prepare_receipts(database, request_sha256)
        applies = self._apply_receipts(database, request_sha256)
        aborts = self._abort_receipts(database, request_sha256)
        return CoordinatorStateSnapshotV2.create(
            request_sha256=request_sha256,
            state=str(row["state"]),
            owner_epoch=int(row["owner_epoch"]),
            prepared_roles=tuple(item.role for item in prepares),
            applied_roles=tuple(item.role for item in applies),
            aborted_roles=tuple(item.role for item in aborts),
            committed_receipt_sha256=(
                None
                if row["committed_receipt_sha256"] is None
                else str(row["committed_receipt_sha256"])
            ),
            abort_receipt_sha256=(
                None
                if row["abort_receipt_sha256"] is None
                else str(row["abort_receipt_sha256"])
            ),
            event_sequence=int(meta["next_event_sequence"]) - 1,
            head_event_sha256=str(meta["head_event_sha256"]),
            updated_at=float(row["updated_at"]),
        )

    def load_state_v2(
        self,
        request_sha256: str,
    ) -> CoordinatorStateSnapshotV2 | None:
        request = require_sha256(
            request_sha256, "coordinator state request SHA-256"
        )
        with self._lock, closing(self._connect()) as database:
            self._validate(database)
            row = database.execute(
                "SELECT 1 FROM coordinator_requests WHERE request_sha256=?",
                (request,),
            ).fetchone()
            if row is None:
                return None
            return self._snapshot(database, request)

    def _reproof_candidates(self, request: SourceReclamationRequestV2) -> None:
        if request.candidate_authority_sha256 != self.candidate_authority_sha256:
            raise AcquisitionSourceReclamationApplyConflict(
                "coordinator request candidate authority differs"
            )
        cursor = 0
        while cursor < request.candidate_count:
            with self._candidate_authority.hold_candidate_page_v1(
                request,
                after_cursor=cursor,
                max_candidates=min(64, request.candidate_count - cursor),
            ) as page:
                if (
                    page.after_cursor != cursor
                    or page.next_cursor <= cursor
                    or page.total_candidate_count != request.candidate_count
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "coordinator candidate page cursor differs"
                    )
                cursor = page.next_cursor

    def _participants_by_role(
        self,
        participants: tuple[SourceReclamationParticipantV2, ...],
    ) -> dict[str, SourceReclamationParticipantV2]:
        if (
            not isinstance(participants, tuple)
            or len(participants) != len(PARTICIPANT_APPLY_ORDER)
            or any(
                not isinstance(item, SourceReclamationParticipantV2)
                for item in participants
            )
            or tuple(item.role for item in participants)
            != PARTICIPANT_APPLY_ORDER
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator participant catalogue differs"
            )
        stores: set[str] = set()
        result: dict[str, SourceReclamationParticipantV2] = {}
        for participant in participants:
            store_identity = require_sha256(
                participant.source_store_identity_sha256,
                "participant source store identity SHA-256",
            )
            if store_identity in stores:
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator participant stores are not disjoint"
                )
            stores.add(store_identity)
            if (
                participant.coordinator_authority_sha256 != self.authority_sha256
                or participant.candidate_authority_sha256
                != self.candidate_authority_sha256
            ):
                raise AcquisitionSourceReclamationApplyConflict(
                    "participant constructor-bound authority differs"
                )
            result[participant.role] = participant
        return result

    def _validate_prepare_receipt(
        self,
        request: SourceReclamationRequestV2,
        receipt: SourceLocalPrepareReceiptV2,
        *,
        expected_role: str | None = None,
        expected_store_identity_sha256: str | None = None,
    ) -> None:
        if not isinstance(receipt, SourceLocalPrepareReceiptV2):
            raise TypeError("prepare receipt must be SourceLocalPrepareReceiptV2")
        if (
            receipt.request_sha256 != request.request_sha256
            or receipt.coordinator_authority_sha256 != self.authority_sha256
            or receipt.candidate_authority_sha256
            != request.candidate_authority_sha256
            or receipt.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or receipt.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or receipt.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or receipt.consumer_registry_root_sha256
            != request.consumer_registry_root_sha256
            or receipt.consumer_epoch_root_sha256
            != request.consumer_epoch_root_sha256
            or receipt.candidate_count != request.candidate_count
            or (expected_role is not None and receipt.role != expected_role)
            or (
                expected_store_identity_sha256 is not None
                and receipt.source_store_identity_sha256
                != expected_store_identity_sha256
            )
        ):
            raise AcquisitionSourceReclamationApplyConflict(
                "source-local prepare receipt differs from request/authority"
            )

    def begin_preparing_v2(
        self,
        request: SourceReclamationRequestV2,
    ) -> CoordinatorStateSnapshotV2:
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        self._reproof_candidates(request)
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                meta = self._validate(database)
                existing = database.execute(
                    "SELECT * FROM coordinator_requests WHERE request_sha256=?",
                    (request.request_sha256,),
                ).fetchone()
                if existing is not None:
                    stored = _request_from_blob(existing["request_json"])
                    if stored != request:
                        raise AcquisitionSourceReclamationApplyConflict(
                            "coordinator request identity was reused"
                        )
                    snapshot = self._snapshot(database, request.request_sha256)
                    database.commit()
                    return snapshot
                if int(database.execute(
                    "SELECT count(*) FROM coordinator_requests"
                ).fetchone()[0]) >= self._limits.max_requests:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "coordinator request cap reached"
                    )
                if database.execute(
                    "SELECT 1 FROM coordinator_requests WHERE active_slot=1"
                ).fetchone() is not None:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "another coordinator request is active"
                    )
                owner_epoch = int(meta["next_owner_epoch"])
                sequence, previous, event_sha, observed = self._event_context(
                    database,
                    request_sha256=request.request_sha256,
                    event_type="preparing",
                    state=PREPARING,
                    payload_sha256=request.request_sha256,
                    owner_epoch=owner_epoch,
                )
                database.execute(
                    "INSERT INTO coordinator_requests VALUES(?,?,?, ?,1,NULL,NULL,NULL,NULL,NULL,NULL,NULL,?,?,NULL)",
                    (
                        request.request_sha256,
                        sqlite3.Binary(canonical_bytes(request.as_dict())),
                        PREPARING,
                        owner_epoch,
                        observed,
                        observed,
                    ),
                )
                database.execute(
                    "UPDATE coordinator_meta SET next_owner_epoch=? WHERE singleton=1",
                    (owner_epoch + 1,),
                )
                self._write_event(
                    database,
                    event_sequence=sequence,
                    event_sha256=event_sha,
                    previous_event_sha256=previous,
                    request_sha256=request.request_sha256,
                    event_type="preparing",
                    state=PREPARING,
                    payload_sha256=request.request_sha256,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                self._fault("begin_preparing.before_commit")
                database.commit()
                self._fault("begin_preparing.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        loaded = self.load_state_v2(request.request_sha256)
        if loaded is None:  # pragma: no cover - committed insert invariant
            raise AcquisitionSourceReclamationApplyDurabilityError(
                "PREPARING state is unavailable after commit"
            )
        return loaded

    def record_source_local_prepare_v2(
        self,
        receipt: SourceLocalPrepareReceiptV2,
    ) -> CoordinatorStateSnapshotV2:
        request_sha = require_sha256(
            receipt.request_sha256, "record prepare request SHA-256"
        )
        encoded = canonical_bytes(receipt.as_dict())
        if len(encoded) > self._limits.max_receipt_bytes:
            raise AcquisitionSourceReclamationApplyCapacityError(
                "prepare receipt exceeds coordinator byte cap"
            )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                row = self._request_row(database, request_sha)
                request = _request_from_blob(row["request_json"])
                self._validate_prepare_receipt(request, receipt)
                if str(row["state"]) != PREPARING:
                    existing = database.execute(
                        "SELECT receipt_json FROM coordinator_prepares "
                        "WHERE request_sha256=? AND role=?",
                        (request_sha, receipt.role),
                    ).fetchone()
                    if existing is not None and bytes(existing[0]) == encoded:
                        snapshot = self._snapshot(database, request_sha)
                        database.commit()
                        return snapshot
                    raise AcquisitionSourceReclamationApplyConflict(
                        "coordinator is no longer accepting prepares"
                    )
                existing = database.execute(
                    "SELECT receipt_json FROM coordinator_prepares "
                    "WHERE request_sha256=? AND role=?",
                    (request_sha, receipt.role),
                ).fetchone()
                if existing is not None:
                    if bytes(existing[0]) != encoded:
                        raise AcquisitionSourceReclamationApplyConflict(
                            "prepare role identity was reused"
                        )
                    snapshot = self._snapshot(database, request_sha)
                    database.commit()
                    return snapshot
                other = database.execute(
                    "SELECT role FROM coordinator_prepares WHERE request_sha256=? "
                    "AND source_store_identity_sha256=?",
                    (request_sha, receipt.source_store_identity_sha256),
                ).fetchone()
                if other is not None:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "two prepare roles bind one source store"
                    )
                observed = self._observed(database)
                database.execute(
                    "INSERT INTO coordinator_prepares VALUES(?,?,?,?,?,?,?)",
                    (
                        request_sha,
                        receipt.role,
                        receipt.source_store_identity_sha256,
                        receipt.prepare_receipt_sha256,
                        receipt.source_fence_sha256,
                        sqlite3.Binary(encoded),
                        observed,
                    ),
                )
                database.execute(
                    "UPDATE coordinator_requests SET updated_at=? "
                    "WHERE request_sha256=?",
                    (observed, request_sha),
                )
                database.execute(
                    "UPDATE coordinator_meta SET last_observed_at=? WHERE singleton=1",
                    (observed,),
                )
                self._fault("record_prepare.before_commit")
                database.commit()
                self._fault("record_prepare.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        loaded = self.load_state_v2(request_sha)
        if loaded is None:  # pragma: no cover
            raise AcquisitionSourceReclamationApplyDurabilityError(
                "recorded prepare is unavailable"
            )
        return loaded

    def finish_prepared_v2(
        self,
        request_sha256: str,
    ) -> CoordinatorStateSnapshotV2:
        request = require_sha256(
            request_sha256, "finish prepared request SHA-256"
        )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                row = self._request_row(database, request)
                if str(row["state"]) in {
                    PREPARED, COMMITTED, APPLIED, ABORTING, ABORTED
                }:
                    snapshot = self._snapshot(database, request)
                    database.commit()
                    return snapshot
                receipts = self._prepare_receipts(database, request)
                if tuple(item.role for item in receipts) != PARTICIPANT_APPLY_ORDER:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "not every participant is durably prepared"
                    )
                bindings = self._prepare_bindings(receipts)
                root = digest_mapping([item.as_dict() for item in bindings])
                owner_epoch = int(row["owner_epoch"])
                sequence, previous, event_sha, observed = self._event_context(
                    database,
                    request_sha256=request,
                    event_type="prepared",
                    state=PREPARED,
                    payload_sha256=root,
                    owner_epoch=owner_epoch,
                )
                database.execute(
                    "UPDATE coordinator_requests SET state=?,"
                    "participant_prepare_root_sha256=?,updated_at=? "
                    "WHERE request_sha256=? AND state=?",
                    (PREPARED, root, observed, request, PREPARING),
                )
                self._write_event(
                    database,
                    event_sequence=sequence,
                    event_sha256=event_sha,
                    previous_event_sha256=previous,
                    request_sha256=request,
                    event_type="prepared",
                    state=PREPARED,
                    payload_sha256=root,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                self._fault("finish_prepared.before_commit")
                database.commit()
                self._fault("finish_prepared.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        loaded = self.load_state_v2(request)
        if loaded is None:  # pragma: no cover
            raise AcquisitionSourceReclamationApplyDurabilityError(
                "PREPARED state is unavailable"
            )
        return loaded

    def prepare_v2(
        self,
        request: SourceReclamationRequestV2,
        *,
        participants: tuple[SourceReclamationParticipantV2, ...],
    ) -> CoordinatorStateSnapshotV2:
        if not isinstance(request, SourceReclamationRequestV2):
            raise TypeError("request must be SourceReclamationRequestV2")
        catalogue = self._participants_by_role(participants)
        existing = self.load_state_v2(request.request_sha256)
        if existing is not None and existing.state in {COMMITTED, APPLIED}:
            return existing
        with self._candidate_authority.hold_current_request_authority_v1(
            request
        ):
            state = self.begin_preparing_v2(request)
            if state.state != PREPARING:
                return state
            for role in PARTICIPANT_APPLY_ORDER:
                participant = catalogue[role]
                receipt = participant.prepare_source_local_v2(request)
                self._validate_prepare_receipt(
                    request,
                    receipt,
                    expected_role=role,
                    expected_store_identity_sha256=(
                        participant.source_store_identity_sha256
                    ),
                )
                self.record_source_local_prepare_v2(receipt)
            return self.finish_prepared_v2(request.request_sha256)

    @staticmethod
    def _committed_receipt_from_row(
        row: sqlite3.Row,
    ) -> CommittedReclamationDecisionReceiptV1:
        if row["committed_receipt_json"] is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator COMMITTED receipt is unavailable"
            )
        receipt = _committed_from_blob(row["committed_receipt_json"])
        if (
            receipt.request_sha256 != str(row["request_sha256"])
            or receipt.decision_sha256 != str(row["committed_decision_sha256"])
            or receipt.receipt_sha256 != str(row["committed_receipt_sha256"])
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator COMMITTED row differs"
            )
        return receipt

    @staticmethod
    def _abort_decision_from_row(
        row: sqlite3.Row,
    ) -> CoordinatorAbortDecisionReceiptV1:
        if row["abort_receipt_json"] is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator ABORTING receipt is unavailable"
            )
        receipt = _coordinator_abort_from_blob(row["abort_receipt_json"])
        if (
            receipt.request_sha256 != str(row["request_sha256"])
            or receipt.abort_decision_sha256 != str(row["abort_decision_sha256"])
            or receipt.receipt_sha256 != str(row["abort_receipt_sha256"])
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator ABORTING row differs"
            )
        return receipt

    def _reproof_participant_prepares(
        self,
        request: SourceReclamationRequestV2,
        participants: dict[str, SourceReclamationParticipantV2],
    ) -> tuple[SourceLocalPrepareReceiptV2, ...]:
        with self._lock, closing(self._connect()) as database:
            self._validate(database)
            row = self._request_row(database, request.request_sha256)
            if str(row["state"]) not in {PREPARED, COMMITTED, APPLIED}:
                raise AcquisitionSourceReclamationApplyConflict(
                    "coordinator request is not PREPARED"
                )
            durable = self._prepare_receipts(database, request.request_sha256)
        if tuple(item.role for item in durable) != PARTICIPANT_APPLY_ORDER:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator durable prepare inventory differs"
            )
        reloaded: list[SourceLocalPrepareReceiptV2] = []
        for stored in durable:
            participant = participants[stored.role]
            current = participant.load_source_local_prepare_v2(
                request.request_sha256
            )
            if current is None:
                raise AcquisitionSourceReclamationApplyConflict(
                    "participant durable prepare disappeared"
                )
            self._validate_prepare_receipt(
                request,
                current,
                expected_role=stored.role,
                expected_store_identity_sha256=(
                    participant.source_store_identity_sha256
                ),
            )
            if current != stored:
                raise AcquisitionSourceReclamationApplyConflict(
                    "participant durable prepare differs from coordinator"
                )
            reloaded.append(current)
        return tuple(reloaded)

    def commit_v1(
        self,
        request_sha256: str,
        *,
        participants: tuple[SourceReclamationParticipantV2, ...],
    ) -> CommittedReclamationDecisionReceiptV1:
        request_sha = require_sha256(
            request_sha256, "commit request SHA-256"
        )
        catalogue = self._participants_by_role(participants)
        with self._lock, closing(self._connect()) as database:
            self._validate(database)
            initial = self._request_row(database, request_sha)
            request = _request_from_blob(initial["request_json"])
            if str(initial["state"]) in {COMMITTED, APPLIED}:
                return self._committed_receipt_from_row(initial)
            if str(initial["state"]) != PREPARED:
                raise AcquisitionSourceReclamationApplyConflict(
                    "coordinator request is not PREPARED for commit"
                )
        with self._candidate_authority.hold_current_request_authority_v1(
            request
        ):
            return self._commit_prepared_under_current_authority_v1(
                request,
                catalogue,
            )

    def _commit_prepared_under_current_authority_v1(
        self,
        request: SourceReclamationRequestV2,
        catalogue: dict[str, SourceReclamationParticipantV2],
    ) -> CommittedReclamationDecisionReceiptV1:
        request_sha = request.request_sha256
        reloaded = self._reproof_participant_prepares(request, catalogue)
        bindings = self._prepare_bindings(reloaded)
        prepare_root = digest_mapping([item.as_dict() for item in bindings])
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                meta = self._validate(database)
                row = self._request_row(database, request_sha)
                if str(row["state"]) in {COMMITTED, APPLIED}:
                    receipt = self._committed_receipt_from_row(row)
                    database.commit()
                    return receipt
                if (
                    str(row["state"]) != PREPARED
                    or str(row["participant_prepare_root_sha256"])
                    != prepare_root
                    or self._prepare_receipts(database, request_sha) != reloaded
                ):
                    raise AcquisitionSourceReclamationApplyConflict(
                        "coordinator PREPARED evidence changed before commit"
                    )
                sequence = int(meta["next_event_sequence"])
                if sequence > self._limits.max_events:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "coordinator event cap reached"
                    )
                previous = str(meta["head_event_sha256"])
                observed = self._observed(database)
                owner_epoch = int(row["owner_epoch"])
                provisional = CommittedReclamationDecisionReceiptV1.create(
                    request_sha256=request.request_sha256,
                    coordinator_store_identity_sha256=self.store_identity_sha256,
                    sealed_archive_anchor_sha256=(
                        request.sealed_archive_anchor_sha256
                    ),
                    archive_binding_root_sha256=request.archive_binding_root_sha256,
                    candidate_bindings_sha256=request.candidate_bindings_sha256,
                    candidate_authority_sha256=request.candidate_authority_sha256,
                    consumer_registry_root_sha256=request.consumer_registry_root_sha256,
                    consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
                    participant_prepares=bindings,
                    candidate_count=request.candidate_count,
                    owner_epoch=owner_epoch,
                    committed_at=observed,
                    commit_event_sequence=sequence,
                    previous_event_sha256=previous,
                    commit_event_sha256=_ZERO_SHA256,
                )
                event_sha = self._event_sha256(
                    event_sequence=sequence,
                    previous_event_sha256=previous,
                    request_sha256=request_sha,
                    event_type="committed",
                    state=COMMITTED,
                    payload_sha256=provisional.decision_sha256,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                receipt = CommittedReclamationDecisionReceiptV1.create(
                    request_sha256=request.request_sha256,
                    coordinator_store_identity_sha256=self.store_identity_sha256,
                    sealed_archive_anchor_sha256=(
                        request.sealed_archive_anchor_sha256
                    ),
                    archive_binding_root_sha256=request.archive_binding_root_sha256,
                    candidate_bindings_sha256=request.candidate_bindings_sha256,
                    candidate_authority_sha256=request.candidate_authority_sha256,
                    consumer_registry_root_sha256=request.consumer_registry_root_sha256,
                    consumer_epoch_root_sha256=request.consumer_epoch_root_sha256,
                    participant_prepares=bindings,
                    candidate_count=request.candidate_count,
                    owner_epoch=owner_epoch,
                    committed_at=observed,
                    commit_event_sequence=sequence,
                    previous_event_sha256=previous,
                    commit_event_sha256=event_sha,
                )
                if receipt.decision_sha256 != provisional.decision_sha256:
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "commit event construction changed decision identity"
                    )
                encoded = canonical_bytes(receipt.as_dict())
                if len(encoded) > self._limits.max_receipt_bytes:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "committed receipt exceeds coordinator byte cap"
                    )
                database.execute(
                    "UPDATE coordinator_requests SET state=?,"
                    "committed_decision_sha256=?,committed_receipt_sha256=?,"
                    "committed_receipt_json=?,updated_at=? WHERE request_sha256=? "
                    "AND state=?",
                    (
                        COMMITTED,
                        receipt.decision_sha256,
                        receipt.receipt_sha256,
                        sqlite3.Binary(encoded),
                        observed,
                        request_sha,
                        PREPARED,
                    ),
                )
                self._write_event(
                    database,
                    event_sequence=sequence,
                    event_sha256=event_sha,
                    previous_event_sha256=previous,
                    request_sha256=request_sha,
                    event_type="committed",
                    state=COMMITTED,
                    payload_sha256=receipt.decision_sha256,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                self._fault("commit.before_commit")
                database.commit()
                self._fault("commit.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        return receipt

    def _held_decision_state(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
        *,
        expected_state: str,
    ) -> tuple[
        sqlite3.Row,
        CommittedReclamationDecisionReceiptV1,
        tuple[SourceLocalApplyReceiptV1, ...],
    ]:
        row = self._request_row(database, request_sha256)
        if str(row["state"]) != expected_state:
            raise AcquisitionSourceReclamationApplyConflict(
                f"coordinator no longer holds {expected_state}"
            )
        receipt = self._committed_receipt_from_row(row)
        request = _request_from_blob(row["request_json"])
        if (
            receipt.coordinator_store_identity_sha256
            != self.store_identity_sha256
            or receipt.candidate_authority_sha256
            != self.candidate_authority_sha256
            or receipt.request_sha256 != request.request_sha256
            or receipt.sealed_archive_anchor_sha256
            != request.sealed_archive_anchor_sha256
            or receipt.archive_binding_root_sha256
            != request.archive_binding_root_sha256
            or receipt.candidate_bindings_sha256
            != request.candidate_bindings_sha256
            or receipt.consumer_registry_root_sha256
            != request.consumer_registry_root_sha256
            or receipt.consumer_epoch_root_sha256
            != request.consumer_epoch_root_sha256
            or receipt.candidate_count != request.candidate_count
            or receipt.owner_epoch != int(row["owner_epoch"])
            or receipt.participant_prepare_root_sha256
            != str(row["participant_prepare_root_sha256"])
            or row["abort_decision_sha256"] is not None
            or row["abort_receipt_sha256"] is not None
            or row["abort_receipt_json"] is not None
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held committed decision/request authority differs"
            )
        commit_event = database.execute(
            "SELECT * FROM coordinator_events WHERE event_sequence=?",
            (receipt.commit_event_sequence,),
        ).fetchone()
        expected_commit_event = self._event_sha256(
            event_sequence=receipt.commit_event_sequence,
            previous_event_sha256=receipt.previous_event_sha256,
            request_sha256=request_sha256,
            event_type="committed",
            state=COMMITTED,
            payload_sha256=receipt.decision_sha256,
            owner_epoch=receipt.owner_epoch,
            observed_at=receipt.committed_at,
        )
        if (
            commit_event is None
            or str(commit_event["event_sha256"]) != expected_commit_event
            or receipt.commit_event_sha256 != expected_commit_event
            or str(commit_event["previous_event_sha256"])
            != receipt.previous_event_sha256
            or str(commit_event["request_sha256"]) != request_sha256
            or str(commit_event["event_type"]) != "committed"
            or str(commit_event["state"]) != COMMITTED
            or str(commit_event["payload_sha256"]) != receipt.decision_sha256
            or int(commit_event["owner_epoch"]) != receipt.owner_epoch
            or float(commit_event["observed_at"]) != receipt.committed_at
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held committed decision event differs"
            )
        prepares = self._prepare_bindings(
            self._prepare_receipts(database, request_sha256)
        )
        if prepares != receipt.participant_prepares:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held COMMITTED participant bindings differ"
            )
        progress = self._apply_receipts(database, request_sha256)
        for item in progress:
            binding = next(
                prepare for prepare in prepares if prepare.role == item.role
            )
            if (
                item.prepare_receipt_sha256 != binding.prepare_receipt_sha256
                or item.source_store_identity_sha256
                != binding.source_store_identity_sha256
                or item.committed_decision_sha256 != receipt.decision_sha256
                or item.committed_decision_receipt_sha256
                != receipt.receipt_sha256
            ):
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "held COMMITTED apply progress differs"
                )
        if self._abort_receipts(database, request_sha256):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held committed decision contains abort progress"
            )
        return row, receipt, progress

    def _held_committed_state(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[
        CommittedReclamationDecisionReceiptV1,
        tuple[SourceLocalApplyReceiptV1, ...],
    ]:
        row, receipt, progress = self._held_decision_state(
            database,
            request_sha256,
            expected_state=COMMITTED,
        )
        if row["active_slot"] != 1 or row["completed_at"] is not None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held COMMITTED lifecycle fields differ"
            )
        return receipt, progress

    def _held_applied_replay_state(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[
        CommittedReclamationDecisionReceiptV1,
        tuple[SourceLocalApplyReceiptV1, ...],
    ]:
        row, receipt, progress = self._held_decision_state(
            database,
            request_sha256,
            expected_state=APPLIED,
        )
        if tuple(item.role for item in progress) != PARTICIPANT_APPLY_ORDER:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held APPLIED progress is incomplete"
            )
        completed_at_value = row["completed_at"]
        if row["active_slot"] is not None or completed_at_value is None:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held APPLIED lifecycle fields differ"
            )
        completed_at = _finite_timestamp(
            completed_at_value,
            label="held APPLIED completion clock",
        )
        applied_events = database.execute(
            "SELECT * FROM coordinator_events WHERE request_sha256=? "
            "AND event_type='applied' ORDER BY event_sequence",
            (request_sha256,),
        ).fetchall()
        apply_root = digest_mapping([
            item.apply_receipt_sha256 for item in progress
        ])
        if len(applied_events) != 1:
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held APPLIED event inventory differs"
            )
        applied_event = applied_events[0]
        expected_event = self._event_sha256(
            event_sequence=int(applied_event["event_sequence"]),
            previous_event_sha256=str(
                applied_event["previous_event_sha256"]
            ),
            request_sha256=request_sha256,
            event_type="applied",
            state=APPLIED,
            payload_sha256=apply_root,
            owner_epoch=receipt.owner_epoch,
            observed_at=float(completed_at),
        )
        if (
            str(applied_event["event_sha256"]) != expected_event
            or int(applied_event["event_sequence"])
            != receipt.commit_event_sequence + 1
            or str(applied_event["previous_event_sha256"])
            != receipt.commit_event_sha256
            or str(applied_event["request_sha256"]) != request_sha256
            or str(applied_event["event_type"]) != "applied"
            or str(applied_event["state"]) != APPLIED
            or str(applied_event["payload_sha256"]) != apply_root
            or int(applied_event["owner_epoch"]) != receipt.owner_epoch
            or float(applied_event["observed_at"]) != float(completed_at)
            or float(row["updated_at"]) != float(completed_at)
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held APPLIED terminal event differs"
            )
        return receipt, progress

    def _held_apply_authority_state(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
        *,
        applying_role: str,
        expected_predecessor_apply_receipt_sha256: str | None,
    ) -> tuple[
        CommittedReclamationDecisionReceiptV1,
        tuple[SourceLocalApplyReceiptV1, ...],
    ]:
        state = str(self._request_row(database, request_sha256)["state"])
        role_index = PARTICIPANT_APPLY_ORDER.index(applying_role)
        if state == COMMITTED:
            receipt, progress = self._held_committed_state(
                database, request_sha256
            )
            expected_roles = PARTICIPANT_APPLY_ORDER[:role_index]
            predecessor = (
                None if not progress else progress[-1].apply_receipt_sha256
            )
            if (
                tuple(item.role for item in progress) != expected_roles
                or expected_predecessor_apply_receipt_sha256 != predecessor
            ):
                raise AcquisitionSourceReclamationApplyConflict(
                    "held coordinator predecessor progress differs"
                )
            return receipt, progress
        if state == APPLIED:
            receipt, progress = self._held_applied_replay_state(
                database, request_sha256
            )
            applied = progress[role_index]
            if (
                applied.role != applying_role
                or applied.predecessor_apply_receipt_sha256
                != expected_predecessor_apply_receipt_sha256
            ):
                raise AcquisitionSourceReclamationApplyConflict(
                    "held APPLIED replay predecessor differs"
                )
            return receipt, progress
        raise AcquisitionSourceReclamationApplyConflict(
            "coordinator holds neither COMMITTED nor APPLIED replay authority"
        )

    def _held_decision_durable_bytes(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
    ) -> tuple[bytes, ...]:
        """Capture the exact durable rows protected by one decision hold."""

        meta = database.execute(
            "SELECT * FROM coordinator_meta WHERE singleton=1"
        ).fetchone()
        request = self._request_row(database, request_sha256)
        if meta is None:  # pragma: no cover - _validate already checked this
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "coordinator hold metadata is unavailable"
            )
        result = [
            _sqlite_exact_row_bytes(meta, _COORDINATOR_COLUMNS["coordinator_meta"]),
            _sqlite_exact_row_bytes(
                request, _COORDINATOR_COLUMNS["coordinator_requests"]
            ),
        ]
        for table in (
            "coordinator_prepares",
            "coordinator_applies",
            "coordinator_aborts",
        ):
            rows = database.execute(
                f"SELECT * FROM {table} WHERE request_sha256=? ORDER BY role",
                (request_sha256,),
            ).fetchall()
            result.extend(
                _sqlite_exact_row_bytes(row, _COORDINATOR_COLUMNS[table])
                for row in rows
            )
        events = database.execute(
            "SELECT * FROM coordinator_events WHERE request_sha256=? "
            "ORDER BY event_sequence",
            (request_sha256,),
        ).fetchall()
        result.extend(
            _sqlite_exact_row_bytes(
                event, _COORDINATOR_COLUMNS["coordinator_events"]
            )
            for event in events
        )
        return tuple(result)

    def record_source_local_apply_v1(
        self,
        receipt: SourceLocalApplyReceiptV1,
        *,
        participant: SourceReclamationParticipantV2,
    ) -> CoordinatorStateSnapshotV2:
        if not isinstance(receipt, SourceLocalApplyReceiptV1):
            raise TypeError("apply receipt must be SourceLocalApplyReceiptV1")
        if not isinstance(participant, SourceReclamationParticipantV2):
            raise TypeError("participant must implement SourceReclamationParticipantV2")
        physically_loaded = participant.load_source_local_apply_v1(
            receipt.request_sha256
        )
        if (
            participant.role != receipt.role
            or participant.source_store_identity_sha256
            != receipt.source_store_identity_sha256
            or participant.coordinator_authority_sha256 != self.authority_sha256
            or participant.candidate_authority_sha256
            != self.candidate_authority_sha256
            or physically_loaded != receipt
        ):
            raise AcquisitionSourceReclamationApplyConflict(
                "participant physical apply receipt differs"
            )
        encoded = canonical_bytes(receipt.as_dict())
        if len(encoded) > self._limits.max_receipt_bytes:
            raise AcquisitionSourceReclamationApplyCapacityError(
                "apply receipt exceeds coordinator byte cap"
            )
        request = receipt.request_sha256
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                row = self._request_row(database, request)
                if str(row["state"]) != COMMITTED:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "coordinator is no longer COMMITTED"
                    )
                existing = database.execute(
                    "SELECT receipt_json FROM coordinator_applies "
                    "WHERE request_sha256=? AND role=?",
                    (request, receipt.role),
                ).fetchone()
                if existing is not None:
                    if bytes(existing[0]) != encoded:
                        raise AcquisitionSourceReclamationApplyConflict(
                            "apply role identity was reused"
                        )
                    snapshot = self._snapshot(database, request)
                    database.commit()
                    return snapshot
                committed, progress = self._held_committed_state(
                    database, request
                )
                next_role = PARTICIPANT_APPLY_ORDER[len(progress)]
                predecessor = (
                    None if not progress else progress[-1].apply_receipt_sha256
                )
                binding = committed.participant_prepares[len(progress)]
                if (
                    receipt.role != next_role
                    or binding.role != next_role
                    or receipt.predecessor_apply_receipt_sha256 != predecessor
                    or receipt.prepare_receipt_sha256
                    != binding.prepare_receipt_sha256
                    or receipt.source_store_identity_sha256
                    != binding.source_store_identity_sha256
                    or receipt.committed_decision_sha256
                    != committed.decision_sha256
                    or receipt.committed_decision_receipt_sha256
                    != committed.receipt_sha256
                ):
                    raise AcquisitionSourceReclamationApplyConflict(
                        "source-local apply receipt differs from held progress"
                    )
                observed = self._observed(database)
                database.execute(
                    "INSERT INTO coordinator_applies VALUES(?,?,?,?,?,?)",
                    (
                        request,
                        receipt.role,
                        receipt.apply_receipt_sha256,
                        receipt.predecessor_apply_receipt_sha256,
                        sqlite3.Binary(encoded),
                        observed,
                    ),
                )
                database.execute(
                    "UPDATE coordinator_requests SET updated_at=? "
                    "WHERE request_sha256=?",
                    (observed, request),
                )
                database.execute(
                    "UPDATE coordinator_meta SET last_observed_at=? WHERE singleton=1",
                    (observed,),
                )
                self._fault("record_apply.before_commit")
                database.commit()
                self._fault("record_apply.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        loaded = self.load_state_v2(request)
        if loaded is None:  # pragma: no cover
            raise AcquisitionSourceReclamationApplyDurabilityError(
                "recorded apply is unavailable"
            )
        return loaded

    def finish_applied_v1(
        self,
        request_sha256: str,
    ) -> CoordinatorStateSnapshotV2:
        request = require_sha256(
            request_sha256, "finish applied request SHA-256"
        )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                row = self._request_row(database, request)
                if str(row["state"]) == APPLIED:
                    snapshot = self._snapshot(database, request)
                    database.commit()
                    return snapshot
                if str(row["state"]) != COMMITTED:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "coordinator cannot finish APPLIED"
                    )
                applies = self._apply_receipts(database, request)
                if tuple(item.role for item in applies) != PARTICIPANT_APPLY_ORDER:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "not every source-local apply is durable"
                    )
                apply_root = digest_mapping([
                    item.apply_receipt_sha256 for item in applies
                ])
                owner_epoch = int(row["owner_epoch"])
                sequence, previous, event_sha, observed = self._event_context(
                    database,
                    request_sha256=request,
                    event_type="applied",
                    state=APPLIED,
                    payload_sha256=apply_root,
                    owner_epoch=owner_epoch,
                )
                database.execute(
                    "UPDATE coordinator_requests SET state=?,active_slot=NULL,"
                    "updated_at=?,completed_at=? WHERE request_sha256=? AND state=?",
                    (APPLIED, observed, observed, request, COMMITTED),
                )
                self._write_event(
                    database,
                    event_sequence=sequence,
                    event_sha256=event_sha,
                    previous_event_sha256=previous,
                    request_sha256=request,
                    event_type="applied",
                    state=APPLIED,
                    payload_sha256=apply_root,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                self._fault("finish_applied.before_commit")
                database.commit()
                self._fault("finish_applied.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        loaded = self.load_state_v2(request)
        if loaded is None:  # pragma: no cover
            raise AcquisitionSourceReclamationApplyDurabilityError(
                "APPLIED state is unavailable"
            )
        return loaded

    def apply_v1(
        self,
        request_sha256: str,
        *,
        participants: tuple[SourceReclamationParticipantV2, ...],
    ) -> CoordinatorStateSnapshotV2:
        request = require_sha256(request_sha256, "apply request SHA-256")
        catalogue = self._participants_by_role(participants)
        state = self.load_state_v2(request)
        if state is None:
            raise AcquisitionSourceReclamationApplyConflict(
                "apply request is unavailable"
            )
        if state.state == APPLIED:
            return self._reproof_applied_participants_v1(
                request,
                catalogue=catalogue,
                expected_snapshot=state,
            )
        if state.state != COMMITTED:
            raise AcquisitionSourceReclamationApplyConflict(
                "apply request is not COMMITTED"
            )
        predecessor: str | None = None
        for index, role in enumerate(PARTICIPANT_APPLY_ORDER):
            participant = catalogue[role]
            current = self.load_state_v2(request)
            if current is None:  # pragma: no cover
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator apply state disappeared"
                )
            if role in current.applied_roles:
                stored = participant.load_source_local_apply_v1(request)
                with self._lock, closing(self._connect()) as database:
                    row = database.execute(
                        "SELECT receipt_json FROM coordinator_applies "
                        "WHERE request_sha256=? AND role=?",
                        (request, role),
                    ).fetchone()
                if row is None or stored is None or stored != _apply_from_blob(row[0]):
                    raise AcquisitionSourceReclamationApplyConflict(
                        "participant apply restart receipt differs"
                    )
                predecessor = stored.apply_receipt_sha256
                continue
            if len(current.applied_roles) != index:
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator apply resume prefix differs"
                )
            receipt = participant.apply_source_local_v1(
                request,
                predecessor_apply_receipt_sha256=predecessor,
            )
            self.record_source_local_apply_v1(
                receipt,
                participant=participant,
            )
            predecessor = receipt.apply_receipt_sha256
        return self.finish_applied_v1(request)

    def _reproof_applied_participants_v1(
        self,
        request_sha256: str,
        *,
        catalogue: dict[str, SourceReclamationParticipantV2],
        expected_snapshot: CoordinatorStateSnapshotV2,
    ) -> CoordinatorStateSnapshotV2:
        """Reproof all physical local receipts before terminal replay."""

        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                before_snapshot = self._snapshot(database, request_sha256)
                if (
                    before_snapshot.state != APPLIED
                    or before_snapshot != expected_snapshot
                ):
                    raise AcquisitionSourceReclamationApplyConflict(
                        "coordinator APPLIED snapshot changed before replay"
                    )
                committed, progress = self._held_applied_replay_state(
                    database, request_sha256
                )
                before_bytes = self._held_decision_durable_bytes(
                    database, request_sha256
                )
                self._fault("apply_replay.before_participant_reproof")
                for index, role in enumerate(PARTICIPANT_APPLY_ORDER):
                    participant = catalogue[role]
                    durable = progress[index]
                    binding = committed.participant_prepares[index]
                    loaded = participant.load_source_local_apply_v1(
                        request_sha256
                    )
                    durable_row = database.execute(
                        "SELECT receipt_json FROM coordinator_applies "
                        "WHERE request_sha256=? AND role=?",
                        (request_sha256, role),
                    ).fetchone()
                    if (
                        not isinstance(loaded, SourceLocalApplyReceiptV1)
                        or durable_row is None
                        or loaded != durable
                        or canonical_bytes(loaded.as_dict())
                        != bytes(durable_row["receipt_json"])
                        or participant.role != role
                        or participant.source_store_identity_sha256
                        != durable.source_store_identity_sha256
                        or durable.source_store_identity_sha256
                        != binding.source_store_identity_sha256
                        or durable.prepare_receipt_sha256
                        != binding.prepare_receipt_sha256
                        or durable.committed_decision_sha256
                        != committed.decision_sha256
                        or durable.committed_decision_receipt_sha256
                        != committed.receipt_sha256
                        or durable.predecessor_apply_receipt_sha256
                        != (
                            None
                            if index == 0
                            else progress[index - 1].apply_receipt_sha256
                        )
                    ):
                        raise AcquisitionSourceReclamationApplyConflict(
                            "participant APPLIED replay receipt differs"
                        )
                    self._fault(f"apply_replay.after_participant_reproof.{role}")
                self._assert_live()
                self._validate(database)
                repeated_committed, repeated_progress = (
                    self._held_applied_replay_state(
                        database, request_sha256
                    )
                )
                after_bytes = self._held_decision_durable_bytes(
                    database, request_sha256
                )
                after_snapshot = self._snapshot(database, request_sha256)
                if (
                    repeated_committed != committed
                    or repeated_progress != progress
                    or after_bytes != before_bytes
                    or after_snapshot != before_snapshot
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "coordinator APPLIED replay proof changed"
                    )
                self._fault("apply_replay.after_exact_reproof")
                return before_snapshot
            finally:
                if database.in_transaction:
                    database.rollback()

    def begin_aborting_v1(
        self,
        request_sha256: str,
    ) -> CoordinatorAbortDecisionReceiptV1:
        request = require_sha256(
            request_sha256, "begin abort request SHA-256"
        )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                meta = self._validate(database)
                row = self._request_row(database, request)
                state = str(row["state"])
                if state in {ABORTING, ABORTED}:
                    receipt = self._abort_decision_from_row(row)
                    database.commit()
                    return receipt
                if state not in {PREPARING, PREPARED}:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "COMMITTED/APPLIED reclamation cannot be aborted"
                    )
                prepares = self._prepare_receipts(database, request)
                prepare_bindings = self._prepare_bindings(prepares)
                prepare_root = digest_mapping([
                    item.as_dict() for item in prepare_bindings
                ])
                if (
                    state == PREPARED
                    and str(row["participant_prepare_root_sha256"])
                    != prepare_root
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "abort PREPARED root differs"
                    )
                sequence = int(meta["next_event_sequence"])
                if sequence > self._limits.max_events:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "coordinator event cap reached"
                    )
                previous = str(meta["head_event_sha256"])
                observed = self._observed(database)
                owner_epoch = int(row["owner_epoch"])
                provisional = CoordinatorAbortDecisionReceiptV1.create(
                    request_sha256=request,
                    coordinator_store_identity_sha256=self.store_identity_sha256,
                    participant_prepare_root_sha256=prepare_root,
                    owner_epoch=owner_epoch,
                    aborting_at=observed,
                    abort_event_sequence=sequence,
                    previous_event_sha256=previous,
                    abort_event_sha256=_ZERO_SHA256,
                )
                event_sha = self._event_sha256(
                    event_sequence=sequence,
                    previous_event_sha256=previous,
                    request_sha256=request,
                    event_type="aborting",
                    state=ABORTING,
                    payload_sha256=provisional.abort_decision_sha256,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                receipt = CoordinatorAbortDecisionReceiptV1.create(
                    request_sha256=request,
                    coordinator_store_identity_sha256=self.store_identity_sha256,
                    participant_prepare_root_sha256=prepare_root,
                    owner_epoch=owner_epoch,
                    aborting_at=observed,
                    abort_event_sequence=sequence,
                    previous_event_sha256=previous,
                    abort_event_sha256=event_sha,
                )
                if (
                    receipt.abort_decision_sha256
                    != provisional.abort_decision_sha256
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "abort event construction changed decision identity"
                    )
                encoded = canonical_bytes(receipt.as_dict())
                if len(encoded) > self._limits.max_receipt_bytes:
                    raise AcquisitionSourceReclamationApplyCapacityError(
                        "abort decision exceeds coordinator byte cap"
                    )
                database.execute(
                    "UPDATE coordinator_requests SET state=?,"
                    "participant_prepare_root_sha256=?,abort_decision_sha256=?,"
                    "abort_receipt_sha256=?,abort_receipt_json=?,updated_at=? "
                    "WHERE request_sha256=? AND state IN (?,?)",
                    (
                        ABORTING,
                        prepare_root,
                        receipt.abort_decision_sha256,
                        receipt.receipt_sha256,
                        sqlite3.Binary(encoded),
                        observed,
                        request,
                        PREPARING,
                        PREPARED,
                    ),
                )
                self._write_event(
                    database,
                    event_sequence=sequence,
                    event_sha256=event_sha,
                    previous_event_sha256=previous,
                    request_sha256=request,
                    event_type="aborting",
                    state=ABORTING,
                    payload_sha256=receipt.abort_decision_sha256,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                self._fault("begin_aborting.before_commit")
                database.commit()
                self._fault("begin_aborting.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        return receipt

    def _held_aborting_state(
        self,
        database: sqlite3.Connection,
        request_sha256: str,
        role: str,
    ) -> tuple[CoordinatorAbortDecisionReceiptV1, ParticipantPrepareBindingV1]:
        row = self._request_row(database, request_sha256)
        if str(row["state"]) != ABORTING:
            raise AcquisitionSourceReclamationApplyConflict(
                "coordinator no longer holds ABORTING"
            )
        receipt = self._abort_decision_from_row(row)
        prepares = self._prepare_receipts(database, request_sha256)
        bindings = self._prepare_bindings(prepares)
        root = digest_mapping([item.as_dict() for item in bindings])
        if (
            receipt.coordinator_store_identity_sha256
            != self.store_identity_sha256
            or receipt.participant_prepare_root_sha256 != root
            or str(row["participant_prepare_root_sha256"]) != root
        ):
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "held ABORTING authority/root differs"
            )
        binding = next((item for item in bindings if item.role == role), None)
        if binding is None:
            raise AcquisitionSourceReclamationApplyConflict(
                "aborting role has no durable prepare fence"
            )
        return receipt, binding

    def record_source_local_abort_v1(
        self,
        receipt: SourceLocalAbortReceiptV1,
        *,
        participant: SourceReclamationParticipantV2,
    ) -> CoordinatorStateSnapshotV2:
        if not isinstance(receipt, SourceLocalAbortReceiptV1):
            raise TypeError("abort receipt must be SourceLocalAbortReceiptV1")
        if not isinstance(participant, SourceReclamationParticipantV2):
            raise TypeError("participant must implement SourceReclamationParticipantV2")
        physically_loaded = participant.load_source_local_abort_v1(
            receipt.request_sha256
        )
        if (
            participant.role != receipt.role
            or participant.source_store_identity_sha256
            != receipt.source_store_identity_sha256
            or participant.coordinator_authority_sha256 != self.authority_sha256
            or participant.candidate_authority_sha256
            != self.candidate_authority_sha256
            or physically_loaded != receipt
        ):
            raise AcquisitionSourceReclamationApplyConflict(
                "participant physical abort receipt differs"
            )
        encoded = canonical_bytes(receipt.as_dict())
        if len(encoded) > self._limits.max_receipt_bytes:
            raise AcquisitionSourceReclamationApplyCapacityError(
                "abort receipt exceeds coordinator byte cap"
            )
        request = receipt.request_sha256
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                row = self._request_row(database, request)
                existing = database.execute(
                    "SELECT receipt_json FROM coordinator_aborts "
                    "WHERE request_sha256=? AND role=?",
                    (request, receipt.role),
                ).fetchone()
                if existing is not None:
                    if bytes(existing[0]) != encoded:
                        raise AcquisitionSourceReclamationApplyConflict(
                            "abort role identity was reused"
                        )
                    snapshot = self._snapshot(database, request)
                    database.commit()
                    return snapshot
                if str(row["state"]) != ABORTING:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "coordinator is no longer ABORTING"
                    )
                decision, binding = self._held_aborting_state(
                    database, request, receipt.role
                )
                if (
                    receipt.source_store_identity_sha256
                    != binding.source_store_identity_sha256
                    or receipt.prepare_receipt_sha256
                    != binding.prepare_receipt_sha256
                    or receipt.coordinator_abort_decision_sha256
                    != decision.abort_decision_sha256
                ):
                    raise AcquisitionSourceReclamationApplyConflict(
                        "source-local abort differs from held decision"
                    )
                observed = self._observed(database)
                database.execute(
                    "INSERT INTO coordinator_aborts VALUES(?,?,?,?,?)",
                    (
                        request,
                        receipt.role,
                        receipt.abort_receipt_sha256,
                        sqlite3.Binary(encoded),
                        observed,
                    ),
                )
                database.execute(
                    "UPDATE coordinator_requests SET updated_at=? "
                    "WHERE request_sha256=?",
                    (observed, request),
                )
                database.execute(
                    "UPDATE coordinator_meta SET last_observed_at=? WHERE singleton=1",
                    (observed,),
                )
                self._fault("record_abort.before_commit")
                database.commit()
                self._fault("record_abort.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        loaded = self.load_state_v2(request)
        if loaded is None:  # pragma: no cover
            raise AcquisitionSourceReclamationApplyDurabilityError(
                "recorded abort is unavailable"
            )
        return loaded

    def finish_aborted_v1(
        self,
        request_sha256: str,
    ) -> CoordinatorStateSnapshotV2:
        request = require_sha256(
            request_sha256, "finish aborted request SHA-256"
        )
        with self._lock, closing(self._connect()) as database:
            try:
                database.execute("BEGIN IMMEDIATE")
                self._validate(database)
                row = self._request_row(database, request)
                if str(row["state"]) == ABORTED:
                    snapshot = self._snapshot(database, request)
                    database.commit()
                    return snapshot
                if str(row["state"]) != ABORTING:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "coordinator cannot finish ABORTED"
                    )
                prepared_roles = tuple(
                    item.role for item in self._prepare_receipts(database, request)
                )
                aborts = self._abort_receipts(database, request)
                aborted_roles = tuple(item.role for item in aborts)
                if aborted_roles != prepared_roles:
                    raise AcquisitionSourceReclamationApplyConflict(
                        "not every prepared source fence is durably aborted"
                    )
                abort_root = digest_mapping([
                    item.abort_receipt_sha256 for item in aborts
                ])
                owner_epoch = int(row["owner_epoch"])
                sequence, previous, event_sha, observed = self._event_context(
                    database,
                    request_sha256=request,
                    event_type="aborted",
                    state=ABORTED,
                    payload_sha256=abort_root,
                    owner_epoch=owner_epoch,
                )
                database.execute(
                    "UPDATE coordinator_requests SET state=?,active_slot=NULL,"
                    "updated_at=?,completed_at=? WHERE request_sha256=? AND state=?",
                    (ABORTED, observed, observed, request, ABORTING),
                )
                self._write_event(
                    database,
                    event_sequence=sequence,
                    event_sha256=event_sha,
                    previous_event_sha256=previous,
                    request_sha256=request,
                    event_type="aborted",
                    state=ABORTED,
                    payload_sha256=abort_root,
                    owner_epoch=owner_epoch,
                    observed_at=observed,
                )
                self._fault("finish_aborted.before_commit")
                database.commit()
                self._fault("finish_aborted.after_commit")
            except BaseException:
                if database.in_transaction:
                    database.rollback()
                raise
        self._directory_fsync(self.path.parent)
        loaded = self.load_state_v2(request)
        if loaded is None:  # pragma: no cover
            raise AcquisitionSourceReclamationApplyDurabilityError(
                "ABORTED state is unavailable"
            )
        return loaded

    def abort_v1(
        self,
        request_sha256: str,
        *,
        participants: tuple[SourceReclamationParticipantV2, ...],
    ) -> CoordinatorStateSnapshotV2:
        request = require_sha256(request_sha256, "abort request SHA-256")
        catalogue = self._participants_by_role(participants)
        state = self.load_state_v2(request)
        if state is None:
            raise AcquisitionSourceReclamationApplyConflict(
                "abort request is unavailable"
            )
        if state.state == ABORTED:
            return state
        decision = self.begin_aborting_v1(request)
        current = self.load_state_v2(request)
        if current is None:  # pragma: no cover
            raise AcquisitionSourceReclamationApplyIntegrityError(
                "ABORTING state disappeared"
            )
        for role in reversed(current.prepared_roles):
            participant = catalogue[role]
            refreshed = self.load_state_v2(request)
            if refreshed is None:  # pragma: no cover
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "abort progress disappeared"
                )
            if role in refreshed.aborted_roles:
                local = participant.load_source_local_abort_v1(request)
                with self._lock, closing(self._connect()) as database:
                    row = database.execute(
                        "SELECT receipt_json FROM coordinator_aborts "
                        "WHERE request_sha256=? AND role=?",
                        (request, role),
                    ).fetchone()
                if row is None or local is None or local != _abort_from_blob(row[0]):
                    raise AcquisitionSourceReclamationApplyConflict(
                        "participant abort restart receipt differs"
                    )
                continue
            receipt = participant.abort_source_local_v1(
                request,
                expected_abort_decision_sha256=(
                    decision.abort_decision_sha256
                ),
            )
            self.record_source_local_abort_v1(
                receipt,
                participant=participant,
            )
        return self.finish_aborted_v1(request)

    def resume_v2(
        self,
        request: SourceReclamationRequestV2,
        *,
        participants: tuple[SourceReclamationParticipantV2, ...],
        allow_commit: bool,
    ) -> CoordinatorStateSnapshotV2:
        """Idempotently resume one explicitly-authorised v2 state machine."""

        if not isinstance(allow_commit, bool):
            raise TypeError("allow_commit must be bool")
        state = self.load_state_v2(request.request_sha256)
        if state is None or state.state == PREPARING:
            state = self.prepare_v2(request, participants=participants)
        if state.state == PREPARED:
            if not allow_commit:
                return state
            self.commit_v1(
                request.request_sha256,
                participants=participants,
            )
            state = self.load_state_v2(request.request_sha256)
            if state is None:  # pragma: no cover
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "COMMITTED state disappeared"
                )
        if state.state in {COMMITTED, APPLIED}:
            return self.apply_v1(
                request.request_sha256,
                participants=participants,
            )
        if state.state == ABORTING:
            return self.abort_v1(
                request.request_sha256,
                participants=participants,
            )
        return state

    def audit_v2(self) -> dict[str, Any]:
        """Read-only bounded physical audit of the coordinator journal."""

        with self._lock, closing(self._connect()) as database:
            meta = self._validate(database)
            previous = _ZERO_SHA256
            events = database.execute(
                "SELECT * FROM coordinator_events ORDER BY event_sequence"
            ).fetchall()
            for ordinal, event in enumerate(events, start=1):
                expected = self._event_sha256(
                    event_sequence=ordinal,
                    previous_event_sha256=previous,
                    request_sha256=str(event["request_sha256"]),
                    event_type=str(event["event_type"]),
                    state=str(event["state"]),
                    payload_sha256=str(event["payload_sha256"]),
                    owner_epoch=int(event["owner_epoch"]),
                    observed_at=float(event["observed_at"]),
                )
                if (
                    int(event["event_sequence"]) != ordinal
                    or str(event["previous_event_sha256"]) != previous
                    or str(event["event_sha256"]) != expected
                ):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "coordinator event chain differs"
                    )
                previous = expected
            if previous != str(meta["head_event_sha256"]):
                raise AcquisitionSourceReclamationApplyIntegrityError(
                    "coordinator audited event head differs"
                )
            rows = database.execute(
                "SELECT * FROM coordinator_requests ORDER BY owner_epoch"
            ).fetchall()
            states = {
                state: 0
                for state in (
                    PREPARING, PREPARED, COMMITTED, APPLIED, ABORTING, ABORTED
                )
            }
            for row in rows:
                request_sha = str(row["request_sha256"])
                state = str(row["state"])
                states[state] += 1
                prepares = self._prepare_receipts(database, request_sha)
                applies = self._apply_receipts(database, request_sha)
                aborts = self._abort_receipts(database, request_sha)
                if state in {PREPARED, COMMITTED, APPLIED} and tuple(
                    item.role for item in prepares
                ) != PARTICIPANT_APPLY_ORDER:
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "audited PREPARED inventory differs"
                    )
                if state in {COMMITTED, APPLIED}:
                    self._committed_receipt_from_row(row)
                if state == APPLIED and tuple(
                    item.role for item in applies
                ) != PARTICIPANT_APPLY_ORDER:
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "audited APPLIED inventory differs"
                    )
                if state in {ABORTING, ABORTED}:
                    self._abort_decision_from_row(row)
                if state == ABORTED and tuple(
                    item.role for item in aborts
                ) != tuple(item.role for item in prepares):
                    raise AcquisitionSourceReclamationApplyIntegrityError(
                        "audited ABORTED inventory differs"
                    )
            return {
                "active_requests": sum(
                    states[item]
                    for item in (PREPARING, PREPARED, COMMITTED, ABORTING)
                ),
                "event_count": len(events),
                "head_event_sha256": previous,
                "request_count": len(rows),
                "states": states,
                "store_identity_sha256": self.store_identity_sha256,
            }


__all__ = [
    "AcquisitionSourceReclamationApplyAuthorityError",
    "AcquisitionSourceReclamationApplyCapacityError",
    "AcquisitionSourceReclamationApplyClockError",
    "AcquisitionSourceReclamationApplyConflict",
    "AcquisitionSourceReclamationApplyCoordinator",
    "AcquisitionSourceReclamationApplyDurabilityError",
    "AcquisitionSourceReclamationApplyIntegrityError",
    "AcquisitionSourceReclamationCoordinatorLimits",
    "CandidateCatalogSealReceiptV1",
    "CoordinatorStateSnapshotV2",
    "SourceReclamationCandidateCatalogLimits",
    "SqliteHeldCommittedDecisionAuthority",
    "SqliteSourceReclamationCandidateAuthority",
]
