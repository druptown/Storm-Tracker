"""Crash-durable, content-addressed storage for exact upstream payload bytes.

This is an isolated vNext primitive and is not yet connected to providers.
It deliberately stores no URL, headers or credentials.  Providers must supply
one stable, non-secret upstream record identifier and explicit provenance.
"""
from __future__ import annotations

from contextlib import closing
import hashlib
import json
import math
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


RAW_ARTIFACT_SCHEMA_VERSION = 2
RAW_ARTIFACT_RECEIPT_SCHEMA = "raw-artifact-receipt-v1"
RAW_ARTIFACT_SET_MANIFEST_SCHEMA = "raw-artifact-set-manifest-v1"
RAW_ARTIFACT_SET_RECEIPT_SCHEMA = "raw-artifact-set-receipt-v1"


class RawArtifactConflictError(RuntimeError):
    """A stable upstream identity was reused for different immutable facts."""


@dataclass(frozen=True, slots=True)
class RawArtifactMember:
    """One exact upstream member prepared for an atomic artifact-set write."""

    member_id: str
    role: str
    upstream_record_id: str
    payload: bytes = field(repr=False)
    media_type: str
    parser_version: str


@dataclass(frozen=True, slots=True)
class RawArtifactReceipt:
    receipt_id: str
    provider_id: str
    upstream_record_id: str
    content_sha256: str
    receipt_sha256: str
    size_bytes: int
    fetched_at: float
    nominal_at: float | None
    media_type: str
    parser_version: str
    created_at: float


@dataclass(frozen=True, slots=True)
class RawArtifactSetMemberReceipt:
    """Payload-free membership metadata returned for an immutable set."""

    member_id: str
    role: str
    receipt_id: str
    upstream_record_id: str
    content_sha256: str
    receipt_sha256: str
    size_bytes: int
    media_type: str
    parser_version: str


@dataclass(frozen=True, slots=True)
class RawArtifactSetReceipt:
    """Receipt proving one complete, atomically committed artifact set."""

    artifact_set_id: str
    provider_id: str
    upstream_set_id: str
    manifest_sha256: str
    provenance_sha256: str
    set_receipt_sha256: str
    member_count: int
    total_size_bytes: int
    fetched_at: float
    nominal_at: float | None
    created_at: float
    members: tuple[RawArtifactSetMemberReceipt, ...]


class RawArtifactStore:
    """SQLite store with atomic exact-byte receipt and artifact-set replay."""

    _INITIALIZE_ATTEMPTS = 16
    _EXPECTED_TABLES = frozenset({
        "raw_artifact_metadata",
        "raw_artifact_blobs",
        "raw_artifact_receipts",
        "raw_artifact_sets",
        "raw_artifact_set_members",
    })
    _EXPECTED_COLUMNS = {
        "raw_artifact_metadata": frozenset({
            "metadata_key",
            "schema_version",
        }),
        "raw_artifact_blobs": frozenset({
            "content_sha256",
            "payload",
            "size_bytes",
            "created_at",
        }),
        "raw_artifact_receipts": frozenset({
            "receipt_id",
            "provider_id",
            "upstream_record_id",
            "content_sha256",
            "receipt_sha256",
            "fetched_at",
            "nominal_at",
            "media_type",
            "parser_version",
            "created_at",
        }),
        "raw_artifact_sets": frozenset({
            "artifact_set_id",
            "provider_id",
            "upstream_set_id",
            "manifest_sha256",
            "provenance_sha256",
            "set_receipt_sha256",
            "member_count",
            "total_size_bytes",
            "fetched_at",
            "nominal_at",
            "created_at",
        }),
        "raw_artifact_set_members": frozenset({
            "artifact_set_id",
            "member_id",
            "role",
            "receipt_id",
        }),
    }
    _EXPECTED_INDEXES = frozenset({
        "idx_raw_artifact_receipts_time",
        "idx_raw_artifact_receipts_content",
        "idx_raw_artifact_sets_time",
        "idx_raw_artifact_sets_manifest",
        "idx_raw_artifact_set_members_receipt",
    })
    _EXPECTED_INDEX_COLUMNS = {
        "idx_raw_artifact_receipts_time": ("fetched_at",),
        "idx_raw_artifact_receipts_content": ("content_sha256",),
        "idx_raw_artifact_sets_time": ("fetched_at",),
        "idx_raw_artifact_sets_manifest": ("manifest_sha256",),
        "idx_raw_artifact_set_members_receipt": ("receipt_id",),
    }
    _EXPECTED_UNIQUE_KEYS = {
        "raw_artifact_metadata": frozenset({("metadata_key",)}),
        "raw_artifact_blobs": frozenset({("content_sha256",)}),
        "raw_artifact_receipts": frozenset({
            ("receipt_id",),
            ("provider_id", "upstream_record_id"),
        }),
        "raw_artifact_sets": frozenset({
            ("artifact_set_id",),
            ("provider_id", "upstream_set_id"),
        }),
        "raw_artifact_set_members": frozenset({
            ("artifact_set_id", "member_id"),
            ("artifact_set_id", "receipt_id"),
        }),
    }
    _EXPECTED_FOREIGN_KEYS = {
        "raw_artifact_metadata": frozenset(),
        "raw_artifact_blobs": frozenset(),
        "raw_artifact_receipts": frozenset({
            ("content_sha256", "raw_artifact_blobs", "content_sha256"),
        }),
        "raw_artifact_sets": frozenset(),
        "raw_artifact_set_members": frozenset({
            ("artifact_set_id", "raw_artifact_sets", "artifact_set_id"),
            ("receipt_id", "raw_artifact_receipts", "receipt_id"),
        }),
    }
    _EXPECTED_TABLE_SQL = {
        "raw_artifact_metadata": (
            "createtableraw_artifact_metadata("
            "metadata_keytextprimarykeycheck(metadata_key='schema'),"
            "schema_versionintegernotnull)withoutrowid"
        ),
        "raw_artifact_blobs": (
            "createtableraw_artifact_blobs("
            "content_sha256textprimarykeycheck(length(content_sha256)=64),"
            "payloadblobnotnull,"
            "size_bytesintegernotnullcheck(size_bytes>=0),"
            "created_atrealnotnull)withoutrowid"
        ),
        "raw_artifact_receipts": (
            "createtableraw_artifact_receipts("
            "receipt_idtextprimarykey,"
            "provider_idtextnotnull,"
            "upstream_record_idtextnotnull,"
            "content_sha256textnotnullreferences"
            "raw_artifact_blobs(content_sha256),"
            "receipt_sha256textnotnullcheck(length(receipt_sha256)=64),"
            "fetched_atrealnotnull,"
            "nominal_atreal,"
            "media_typetextnotnull,"
            "parser_versiontextnotnull,"
            "created_atrealnotnull,"
            "unique(provider_id,upstream_record_id))withoutrowid"
        ),
        "raw_artifact_sets": (
            "createtableraw_artifact_sets("
            "artifact_set_idtextprimarykey,"
            "provider_idtextnotnull,"
            "upstream_set_idtextnotnull,"
            "manifest_sha256textnotnullcheck(length(manifest_sha256)=64),"
            "provenance_sha256textnotnullcheck(length(provenance_sha256)=64),"
            "set_receipt_sha256textnotnull"
            "check(length(set_receipt_sha256)=64),"
            "member_countintegernotnullcheck(member_count>0),"
            "total_size_bytesintegernotnullcheck(total_size_bytes>=0),"
            "fetched_atrealnotnull,"
            "nominal_atreal,"
            "created_atrealnotnull,"
            "unique(provider_id,upstream_set_id))withoutrowid"
        ),
        "raw_artifact_set_members": (
            "createtableraw_artifact_set_members("
            "artifact_set_idtextnotnullreferences"
            "raw_artifact_sets(artifact_set_id),"
            "member_idtextnotnull,"
            "roletextnotnull,"
            "receipt_idtextnotnullreferences"
            "raw_artifact_receipts(receipt_id),"
            "primarykey(artifact_set_id,member_id),"
            "unique(artifact_set_id,receipt_id))withoutrowid"
        ),
    }
    _EXPECTED_INDEX_SQL = {
        "idx_raw_artifact_receipts_time": (
            "createindexidx_raw_artifact_receipts_time"
            "onraw_artifact_receipts(fetched_at)"
        ),
        "idx_raw_artifact_receipts_content": (
            "createindexidx_raw_artifact_receipts_content"
            "onraw_artifact_receipts(content_sha256)"
        ),
        "idx_raw_artifact_sets_time": (
            "createindexidx_raw_artifact_sets_time"
            "onraw_artifact_sets(fetched_at)"
        ),
        "idx_raw_artifact_sets_manifest": (
            "createindexidx_raw_artifact_sets_manifest"
            "onraw_artifact_sets(manifest_sha256)"
        ),
        "idx_raw_artifact_set_members_receipt": (
            "createindexidx_raw_artifact_set_members_receipt"
            "onraw_artifact_set_members(receipt_id)"
        ),
    }
    _EXPECTED_SCHEMA_OBJECTS = frozenset({
        ("table", "raw_artifact_metadata", "raw_artifact_metadata"),
        ("table", "raw_artifact_blobs", "raw_artifact_blobs"),
        ("table", "raw_artifact_receipts", "raw_artifact_receipts"),
        ("table", "raw_artifact_sets", "raw_artifact_sets"),
        (
            "table",
            "raw_artifact_set_members",
            "raw_artifact_set_members",
        ),
        (
            "index",
            "idx_raw_artifact_receipts_time",
            "raw_artifact_receipts",
        ),
        (
            "index",
            "idx_raw_artifact_receipts_content",
            "raw_artifact_receipts",
        ),
        (
            "index",
            "idx_raw_artifact_sets_time",
            "raw_artifact_sets",
        ),
        (
            "index",
            "idx_raw_artifact_sets_manifest",
            "raw_artifact_sets",
        ),
        (
            "index",
            "idx_raw_artifact_set_members_receipt",
            "raw_artifact_set_members",
        ),
    })

    def __init__(
        self,
        path: str | Path,
        *,
        fault_injector: Callable[[str], None] | None = None,
    ) -> None:
        self.path = Path(path)
        self._lock = threading.RLock()
        self._initialized = False
        self._fault_injector = fault_injector

    def initialize(self) -> dict:
        """Create schema v2 atomically or reject an unknown existing layout.

        An existing database is inspected before WAL mode is enabled.  This
        keeps an unknown, legacy or corrupt database byte-preserving on the
        fail-closed path instead of silently adopting or mutating it.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(self._INITIALIZE_ATTEMPTS):
            try:
                stats = self._initialize_once()
            except sqlite3.OperationalError as exc:
                if (
                    not self._retryable_initialize_error(exc)
                    or attempt + 1 >= self._INITIALIZE_ATTEMPTS
                ):
                    raise
                time.sleep(min(0.01 * (attempt + 1), 0.1))
                continue
            self._initialized = True
            return stats
        raise RuntimeError("raw-artifactinitialisatie werd niet voltooid")

    def _initialize_once(self) -> dict:
        """Run one schema-admission attempt.

        Concurrent creators can observe an empty schema before another
        connection commits it.  The public wrapper retries only the resulting
        SQLite lock/already-exists races and revalidates the complete schema on
        every attempt.
        """

        with self._lock, closing(self._connect_unconfigured()) as db:
            try:
                tables = {
                    str(row["name"])
                    for row in db.execute(
                        """
                        SELECT name
                        FROM sqlite_master
                        WHERE type='table' AND name NOT LIKE 'sqlite_%'
                        """
                    ).fetchall()
                }
            except sqlite3.DatabaseError as exc:
                if self._retryable_initialize_error(exc):
                    raise
                raise RuntimeError(
                    "raw-artifactdatabase is geen leesbaar SQLite-schema"
                ) from exc
            if not tables:
                self._configure_durable_writes(db)
                try:
                    db.executescript(
                        f"""
                        BEGIN IMMEDIATE;
                        CREATE TABLE raw_artifact_metadata (
                            metadata_key TEXT PRIMARY KEY
                                CHECK(metadata_key='schema'),
                            schema_version INTEGER NOT NULL
                        ) WITHOUT ROWID;
                        INSERT INTO raw_artifact_metadata(
                            metadata_key, schema_version
                        ) VALUES('schema', {RAW_ARTIFACT_SCHEMA_VERSION});
                        CREATE TABLE raw_artifact_blobs (
                            content_sha256 TEXT PRIMARY KEY
                                CHECK(length(content_sha256)=64),
                            payload BLOB NOT NULL,
                            size_bytes INTEGER NOT NULL
                                CHECK(size_bytes >= 0),
                            created_at REAL NOT NULL
                        ) WITHOUT ROWID;
                        CREATE TABLE raw_artifact_receipts (
                            receipt_id TEXT PRIMARY KEY,
                            provider_id TEXT NOT NULL,
                            upstream_record_id TEXT NOT NULL,
                            content_sha256 TEXT NOT NULL REFERENCES
                                raw_artifact_blobs(content_sha256),
                            receipt_sha256 TEXT NOT NULL
                                CHECK(length(receipt_sha256)=64),
                            fetched_at REAL NOT NULL,
                            nominal_at REAL,
                            media_type TEXT NOT NULL,
                            parser_version TEXT NOT NULL,
                            created_at REAL NOT NULL,
                            UNIQUE(provider_id, upstream_record_id)
                        ) WITHOUT ROWID;
                        CREATE INDEX idx_raw_artifact_receipts_time
                            ON raw_artifact_receipts(fetched_at);
                        CREATE INDEX idx_raw_artifact_receipts_content
                            ON raw_artifact_receipts(content_sha256);
                        CREATE TABLE raw_artifact_sets (
                            artifact_set_id TEXT PRIMARY KEY,
                            provider_id TEXT NOT NULL,
                            upstream_set_id TEXT NOT NULL,
                            manifest_sha256 TEXT NOT NULL
                                CHECK(length(manifest_sha256)=64),
                            provenance_sha256 TEXT NOT NULL
                                CHECK(length(provenance_sha256)=64),
                            set_receipt_sha256 TEXT NOT NULL
                                CHECK(length(set_receipt_sha256)=64),
                            member_count INTEGER NOT NULL
                                CHECK(member_count > 0),
                            total_size_bytes INTEGER NOT NULL
                                CHECK(total_size_bytes >= 0),
                            fetched_at REAL NOT NULL,
                            nominal_at REAL,
                            created_at REAL NOT NULL,
                            UNIQUE(provider_id, upstream_set_id)
                        ) WITHOUT ROWID;
                        CREATE TABLE raw_artifact_set_members (
                            artifact_set_id TEXT NOT NULL REFERENCES
                                raw_artifact_sets(artifact_set_id),
                            member_id TEXT NOT NULL,
                            role TEXT NOT NULL,
                            receipt_id TEXT NOT NULL REFERENCES
                                raw_artifact_receipts(receipt_id),
                            PRIMARY KEY(artifact_set_id, member_id),
                            UNIQUE(artifact_set_id, receipt_id)
                        ) WITHOUT ROWID;
                        CREATE INDEX idx_raw_artifact_sets_time
                            ON raw_artifact_sets(fetched_at);
                        CREATE INDEX idx_raw_artifact_sets_manifest
                            ON raw_artifact_sets(manifest_sha256);
                        CREATE INDEX idx_raw_artifact_set_members_receipt
                            ON raw_artifact_set_members(receipt_id);
                        COMMIT;
                        """
                    )
                except BaseException:
                    if db.in_transaction:
                        db.rollback()
                    raise
                tables = set(self._EXPECTED_TABLES)
            try:
                self._validate_schema(db, tables)
            except sqlite3.DatabaseError as exc:
                if self._retryable_initialize_error(exc):
                    raise
                raise RuntimeError(
                    "raw-artifactschema is onvolledig of onleesbaar"
                ) from exc
            self._configure_durable_writes(db)
            stats = self._stats(db)
        return stats

    def store(
        self,
        *,
        provider_id: str,
        upstream_record_id: str,
        payload: bytes,
        fetched_at: float,
        nominal_at: float | None,
        media_type: str,
        parser_version: str,
    ) -> RawArtifactReceipt:
        """Atomically store exact bytes and one immutable provenance receipt."""
        self._require_initialized()
        provider_id = self._identifier(provider_id, "provider_id", 100)
        upstream_record_id = self._identifier(
            upstream_record_id,
            "upstream_record_id",
            512,
        )
        media_type = self._identifier(media_type, "media_type", 200)
        parser_version = self._identifier(
            parser_version,
            "parser_version",
            200,
        )
        if not isinstance(payload, bytes):
            raise TypeError("raw artifact payload moet exact bytes zijn")
        if not math.isfinite(fetched_at) or fetched_at < 0:
            raise ValueError("fetched_at moet eindig en niet-negatief zijn")
        if (
            nominal_at is not None
            and (not math.isfinite(nominal_at) or nominal_at < 0)
        ):
            raise ValueError(
                "nominal_at moet eindig, niet-negatief of None zijn"
            )

        content_sha256 = hashlib.sha256(payload).hexdigest()
        receipt_id = self._receipt_id(provider_id, upstream_record_id)
        receipt_sha256 = self._receipt_sha256(
            provider_id=provider_id,
            upstream_record_id=upstream_record_id,
            content_sha256=content_sha256,
            fetched_at=float(fetched_at),
            nominal_at=(
                float(nominal_at) if nominal_at is not None else None
            ),
            media_type=media_type,
            parser_version=parser_version,
        )
        created_at = time.time()
        with self._lock, closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            try:
                self._checkpoint("store_after_begin")
                existing = db.execute(
                    """
                    SELECT receipt.*, blob.size_bytes, blob.payload
                    FROM raw_artifact_receipts AS receipt
                    JOIN raw_artifact_blobs AS blob USING(content_sha256)
                    WHERE receipt.provider_id=?
                      AND receipt.upstream_record_id=?
                    """,
                    (provider_id, upstream_record_id),
                ).fetchone()
                if existing is not None:
                    self._verified_receipt(existing)
                    self._assert_same_receipt(
                        existing,
                        content_sha256=content_sha256,
                        nominal_at=nominal_at,
                        media_type=media_type,
                        parser_version=parser_version,
                    )
                    db.commit()
                    return self._decode_receipt(existing)

                existing_blob = db.execute(
                    """
                    SELECT payload, size_bytes
                    FROM raw_artifact_blobs
                    WHERE content_sha256=?
                    """,
                    (content_sha256,),
                ).fetchone()
                if existing_blob is None:
                    db.execute(
                        """
                        INSERT INTO raw_artifact_blobs(
                            content_sha256, payload, size_bytes, created_at
                        ) VALUES(?, ?, ?, ?)
                        """,
                        (
                            content_sha256,
                            sqlite3.Binary(payload),
                            len(payload),
                            created_at,
                        ),
                    )
                elif (
                    int(existing_blob["size_bytes"]) != len(payload)
                    or bytes(existing_blob["payload"]) != payload
                ):
                    raise RawArtifactConflictError(
                        "SHA-256 blobidentity verwijst naar andere bytes"
                    )
                self._checkpoint("store_after_blob")

                db.execute(
                    """
                    INSERT INTO raw_artifact_receipts(
                        receipt_id, provider_id, upstream_record_id,
                        content_sha256, receipt_sha256, fetched_at, nominal_at,
                        media_type, parser_version, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        receipt_id,
                        provider_id,
                        upstream_record_id,
                        content_sha256,
                        receipt_sha256,
                        float(fetched_at),
                        float(nominal_at) if nominal_at is not None else None,
                        media_type,
                        parser_version,
                        created_at,
                    ),
                )
                self._checkpoint("store_after_receipt")
                row = db.execute(
                    """
                    SELECT receipt.*, blob.size_bytes, blob.payload
                    FROM raw_artifact_receipts AS receipt
                    JOIN raw_artifact_blobs AS blob USING(content_sha256)
                    WHERE receipt.receipt_id=?
                    """,
                    (receipt_id,),
                ).fetchone()
                if row is None:
                    raise RawArtifactConflictError(
                        "raw artifact receipt ontbreekt voor commit"
                    )
                self._verified_receipt(row)
                receipt = self._decode_receipt(row)
                self._checkpoint("store_before_commit")
                db.commit()
                self._checkpoint("store_after_commit")
                return receipt
            except BaseException:
                db.rollback()
                raise

    def store_set(
        self,
        *,
        provider_id: str,
        upstream_set_id: str,
        members: Iterable[RawArtifactMember | Mapping[str, Any]],
        fetched_at: float,
        nominal_at: float | None,
        provenance_sha256: str,
    ) -> RawArtifactSetReceipt:
        """Atomically store every exact member plus immutable set provenance.

        The complete set is committed in one ``BEGIN IMMEDIATE`` transaction.
        A crash therefore exposes either no new member/set records or one
        complete set.  Canonical member ordering makes re-delivery idempotent.
        """

        self._require_initialized()
        provider_id = self._identifier(provider_id, "provider_id", 100)
        upstream_set_id = self._identifier(
            upstream_set_id,
            "upstream_set_id",
            512,
        )
        provenance_sha256 = self._sha256(
            provenance_sha256,
            "provenance_sha256",
        )
        fetched_at, nominal_at = self._timestamps(fetched_at, nominal_at)
        prepared = tuple(sorted(
            (self._member(member) for member in members),
            key=lambda item: item.member_id,
        ))
        if not prepared:
            raise ValueError("raw artifact set moet minstens één member hebben")
        member_ids = [member.member_id for member in prepared]
        if len(set(member_ids)) != len(member_ids):
            raise ValueError("raw artifact set bevat dubbele member_id")
        upstream_ids = [member.upstream_record_id for member in prepared]
        if len(set(upstream_ids)) != len(upstream_ids):
            raise ValueError(
                "raw artifact set bevat dubbele upstream_record_id"
            )

        manifest_sha256 = self._manifest_sha256(prepared)
        artifact_set_id = self._artifact_set_id(
            provider_id,
            upstream_set_id,
        )
        total_size_bytes = sum(len(member.payload) for member in prepared)
        set_receipt_sha256 = self._set_receipt_sha256(
            provider_id=provider_id,
            upstream_set_id=upstream_set_id,
            manifest_sha256=manifest_sha256,
            provenance_sha256=provenance_sha256,
            member_count=len(prepared),
            total_size_bytes=total_size_bytes,
            fetched_at=fetched_at,
            nominal_at=nominal_at,
        )
        created_at = time.time()

        with self._lock, closing(self._connect()) as db, db:
            db.execute("BEGIN IMMEDIATE")
            try:
                self._checkpoint("store_set_after_begin")
                existing = db.execute(
                    """
                    SELECT *
                    FROM raw_artifact_sets
                    WHERE provider_id=? AND upstream_set_id=?
                    """,
                    (provider_id, upstream_set_id),
                ).fetchone()
                if existing is not None:
                    receipt = self._decode_set_receipt(
                        db,
                        existing,
                        verify_payloads=True,
                    )
                    self._assert_same_set(
                        receipt,
                        manifest_sha256=manifest_sha256,
                        provenance_sha256=provenance_sha256,
                        nominal_at=nominal_at,
                        members=prepared,
                    )
                    db.commit()
                    return receipt

                receipt_ids: list[str] = []
                for index, member in enumerate(prepared):
                    receipt = self._store_member_in_transaction(
                        db,
                        provider_id=provider_id,
                        member=member,
                        fetched_at=fetched_at,
                        nominal_at=nominal_at,
                        created_at=created_at,
                    )
                    receipt_ids.append(receipt.receipt_id)
                    if index == 0:
                        self._checkpoint("store_set_after_first_member")
                self._checkpoint("store_set_after_all_member_receipts")

                db.execute(
                    """
                    INSERT INTO raw_artifact_sets(
                        artifact_set_id, provider_id, upstream_set_id,
                        manifest_sha256, provenance_sha256,
                        set_receipt_sha256, member_count, total_size_bytes,
                        fetched_at, nominal_at, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        artifact_set_id,
                        provider_id,
                        upstream_set_id,
                        manifest_sha256,
                        provenance_sha256,
                        set_receipt_sha256,
                        len(prepared),
                        total_size_bytes,
                        fetched_at,
                        nominal_at,
                        created_at,
                    ),
                )
                self._checkpoint("store_set_after_set")
                db.executemany(
                    """
                    INSERT INTO raw_artifact_set_members(
                        artifact_set_id, member_id, role, receipt_id
                    ) VALUES(?, ?, ?, ?)
                    """,
                    (
                        (
                            artifact_set_id,
                            member.member_id,
                            member.role,
                            receipt_id,
                        )
                        for member, receipt_id in zip(
                            prepared,
                            receipt_ids,
                            strict=True,
                        )
                    ),
                )
                self._checkpoint("store_set_after_members")
                row = db.execute(
                    """
                    SELECT *
                    FROM raw_artifact_sets
                    WHERE artifact_set_id=?
                    """,
                    (artifact_set_id,),
                ).fetchone()
                receipt = self._decode_set_receipt(
                    db,
                    row,
                    verify_payloads=True,
                )
                self._assert_same_set(
                    receipt,
                    manifest_sha256=manifest_sha256,
                    provenance_sha256=provenance_sha256,
                    nominal_at=nominal_at,
                    members=prepared,
                )
                self._checkpoint("store_set_before_commit")
                db.commit()
                self._checkpoint("store_set_after_commit")
                return receipt
            except BaseException:
                db.rollback()
                raise

    def load_receipt(self, receipt_id: str) -> RawArtifactReceipt | None:
        self._require_initialized()
        with self._lock, closing(self._connect()) as db, db:
            row = db.execute(
                """
                SELECT receipt.*, blob.size_bytes
                FROM raw_artifact_receipts AS receipt
                JOIN raw_artifact_blobs AS blob USING(content_sha256)
                WHERE receipt.receipt_id=?
                """,
                (str(receipt_id),),
            ).fetchone()
            return self._decode_receipt(row) if row is not None else None

    def load_payload(self, receipt_id: str) -> bytes | None:
        """Return the exact stored upstream bytes for deterministic replay."""
        self._require_initialized()
        with self._lock, closing(self._connect()) as db, db:
            row = db.execute(
                """
                SELECT blob.content_sha256, blob.payload, blob.size_bytes
                FROM raw_artifact_receipts AS receipt
                JOIN raw_artifact_blobs AS blob USING(content_sha256)
                WHERE receipt.receipt_id=?
                """,
                (str(receipt_id),),
            ).fetchone()
            if row is None:
                return None
            return self._verified_payload(row)

    def load_set_receipt(
        self,
        artifact_set_id: str,
    ) -> RawArtifactSetReceipt | None:
        """Load and fully verify one immutable artifact-set receipt."""

        self._require_initialized()
        with self._lock, closing(self._connect()) as db, db:
            row = db.execute(
                """
                SELECT *
                FROM raw_artifact_sets
                WHERE artifact_set_id=?
                """,
                (str(artifact_set_id),),
            ).fetchone()
            if row is None:
                return None
            return self._decode_set_receipt(db, row, verify_payloads=True)

    def load_set_payloads(
        self,
        artifact_set_id: str,
    ) -> tuple[tuple[str, bytes], ...] | None:
        """Return every exact payload only when the complete set verifies."""

        receipt = self.load_set_receipt(artifact_set_id)
        if receipt is None:
            return None
        payloads: list[tuple[str, bytes]] = []
        for member in receipt.members:
            payload = self.load_payload(member.receipt_id)
            if payload is None:
                raise RawArtifactConflictError(
                    "artifact set verwijst naar een ontbrekend payloadreceipt"
                )
            payloads.append((member.member_id, payload))
        return tuple(payloads)

    def stats(self) -> dict:
        self._require_initialized()
        with self._lock, closing(self._connect()) as db, db:
            return self._stats(db)

    def integrity_check(self) -> dict:
        self._require_initialized()
        with self._lock, closing(self._connect()) as db, db:
            return self._integrity_report(db)

    def _validate_schema(
        self,
        db: sqlite3.Connection,
        tables: set[str],
    ) -> None:
        if tables != self._EXPECTED_TABLES:
            raise RuntimeError(
                "onbekend of onvolledig raw-artifactschema; "
                f"gevonden tabellen: {sorted(tables)}"
            )
        schema_objects = self._schema_objects(db)
        if schema_objects != self._EXPECTED_SCHEMA_OBJECTS:
            missing = sorted(self._EXPECTED_SCHEMA_OBJECTS - schema_objects)
            unexpected = sorted(
                schema_objects - self._EXPECTED_SCHEMA_OBJECTS
            )
            raise RuntimeError(
                "raw-artifactschema heeft onverwachte schemaobjecten; "
                f"ontbrekend={missing}, onverwacht={unexpected}"
            )
        rows = db.execute(
            """
            SELECT metadata_key, schema_version
            FROM raw_artifact_metadata
            """
        ).fetchall()
        if (
            len(rows) != 1
            or str(rows[0]["metadata_key"]) != "schema"
        ):
            raise RuntimeError(
                "raw-artifactschema heeft geen unieke metadata-identiteit"
            )
        version = int(rows[0]["schema_version"])
        if version != RAW_ARTIFACT_SCHEMA_VERSION:
            raise RuntimeError(
                "onbekende raw-artifactschemaversie "
                f"{version}; verwacht {RAW_ARTIFACT_SCHEMA_VERSION}"
            )
        for table, expected in self._EXPECTED_COLUMNS.items():
            actual = {
                str(row["name"])
                for row in db.execute(
                    f"PRAGMA table_info({table})"
                ).fetchall()
            }
            if actual != expected:
                raise RuntimeError(
                    f"raw-artifacttabel {table} heeft onverwachte kolommen"
                )
        for table, expected in self._EXPECTED_TABLE_SQL.items():
            row = db.execute(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type='table' AND name=?
                """,
                (table,),
            ).fetchone()
            actual = self._canonical_schema_sql(
                row["sql"] if row is not None else None
            )
            if actual != expected:
                raise RuntimeError(
                    f"raw-artifacttabel {table} heeft onverwachte canonieke DDL"
                )
        indexes = {
            str(row["name"])
            for row in db.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='index' AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        }
        if indexes != self._EXPECTED_INDEXES:
            raise RuntimeError(
                "raw-artifactschema heeft onverwachte indexen: "
                f"{sorted(indexes)}"
            )
        for index_name, expected in self._EXPECTED_INDEX_COLUMNS.items():
            actual = tuple(
                str(row["name"])
                for row in db.execute(
                    f"PRAGMA index_info({index_name})"
                ).fetchall()
            )
            if actual != expected:
                raise RuntimeError(
                    f"raw-artifactindex {index_name} heeft "
                    "onverwachte kolommen"
                )
        for index_name, expected in self._EXPECTED_INDEX_SQL.items():
            row = db.execute(
                """
                SELECT sql
                FROM sqlite_master
                WHERE type='index' AND name=?
                """,
                (index_name,),
            ).fetchone()
            actual = self._canonical_schema_sql(
                row["sql"] if row is not None else None
            )
            if actual != expected:
                raise RuntimeError(
                    f"raw-artifactindex {index_name} heeft "
                    "onverwachte canonieke DDL"
                )
        for table, expected in self._EXPECTED_UNIQUE_KEYS.items():
            actual: set[tuple[str, ...]] = set()
            for index in db.execute(
                f"PRAGMA index_list({table})"
            ).fetchall():
                if int(index["unique"]) != 1:
                    continue
                actual.add(tuple(
                    str(row["name"])
                    for row in db.execute(
                        f"PRAGMA index_info({index['name']})"
                    ).fetchall()
                ))
            if frozenset(actual) != expected:
                raise RuntimeError(
                    f"raw-artifacttabel {table} heeft onverwachte "
                    "unique constraints"
                )
        for table, expected in self._EXPECTED_FOREIGN_KEYS.items():
            actual = frozenset(
                (
                    str(row["from"]),
                    str(row["table"]),
                    str(row["to"]),
                )
                for row in db.execute(
                    f"PRAGMA foreign_key_list({table})"
                ).fetchall()
            )
            if actual != expected:
                raise RuntimeError(
                    f"raw-artifacttabel {table} heeft onverwachte foreign keys"
                )
        report = self._integrity_report(db)
        if not report["healthy"]:
            raise RuntimeError(
                "raw-artifactdatabase faalt integriteitscontrole: "
                + json.dumps(report, sort_keys=True)
            )

    @staticmethod
    def _assert_same_receipt(
        row: sqlite3.Row,
        *,
        content_sha256: str,
        nominal_at: float | None,
        media_type: str,
        parser_version: str,
    ) -> None:
        stored_nominal = (
            float(row["nominal_at"])
            if row["nominal_at"] is not None
            else None
        )
        if (
            str(row["content_sha256"]) != content_sha256
            or stored_nominal != nominal_at
            or str(row["media_type"]) != media_type
            or str(row["parser_version"]) != parser_version
        ):
            raise RawArtifactConflictError(
                "upstream record-ID werd voor andere immutable inhoud "
                "of provenance hergebruikt"
            )

    def _store_member_in_transaction(
        self,
        db: sqlite3.Connection,
        *,
        provider_id: str,
        member: RawArtifactMember,
        fetched_at: float,
        nominal_at: float | None,
        created_at: float,
    ) -> RawArtifactReceipt:
        content_sha256 = hashlib.sha256(member.payload).hexdigest()
        receipt_id = self._receipt_id(
            provider_id,
            member.upstream_record_id,
        )
        receipt_sha256 = self._receipt_sha256(
            provider_id=provider_id,
            upstream_record_id=member.upstream_record_id,
            content_sha256=content_sha256,
            fetched_at=fetched_at,
            nominal_at=nominal_at,
            media_type=member.media_type,
            parser_version=member.parser_version,
        )
        existing = db.execute(
            """
            SELECT receipt.*, blob.size_bytes, blob.payload
            FROM raw_artifact_receipts AS receipt
            JOIN raw_artifact_blobs AS blob USING(content_sha256)
            WHERE receipt.provider_id=?
              AND receipt.upstream_record_id=?
            """,
            (provider_id, member.upstream_record_id),
        ).fetchone()
        if existing is not None:
            self._verified_receipt(existing)
            self._assert_same_receipt(
                existing,
                content_sha256=content_sha256,
                nominal_at=nominal_at,
                media_type=member.media_type,
                parser_version=member.parser_version,
            )
            return self._decode_receipt(existing)

        blob = db.execute(
            """
            SELECT content_sha256, payload, size_bytes
            FROM raw_artifact_blobs
            WHERE content_sha256=?
            """,
            (content_sha256,),
        ).fetchone()
        if blob is None:
            db.execute(
                """
                INSERT INTO raw_artifact_blobs(
                    content_sha256, payload, size_bytes, created_at
                ) VALUES(?, ?, ?, ?)
                """,
                (
                    content_sha256,
                    sqlite3.Binary(member.payload),
                    len(member.payload),
                    created_at,
                ),
            )
        elif (
            not self._blob_matches(blob)
            or bytes(blob["payload"]) != member.payload
        ):
            raise RawArtifactConflictError(
                "SHA-256 blobidentity verwijst naar andere bytes"
            )

        db.execute(
            """
            INSERT INTO raw_artifact_receipts(
                receipt_id, provider_id, upstream_record_id,
                content_sha256, receipt_sha256, fetched_at, nominal_at,
                media_type, parser_version, created_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt_id,
                provider_id,
                member.upstream_record_id,
                content_sha256,
                receipt_sha256,
                fetched_at,
                nominal_at,
                member.media_type,
                member.parser_version,
                created_at,
            ),
        )
        row = db.execute(
            """
            SELECT receipt.*, blob.size_bytes
            FROM raw_artifact_receipts AS receipt
            JOIN raw_artifact_blobs AS blob USING(content_sha256)
            WHERE receipt.receipt_id=?
            """,
            (receipt_id,),
        ).fetchone()
        return self._decode_receipt(row)

    @classmethod
    def _member(
        cls,
        value: RawArtifactMember | Mapping[str, Any],
    ) -> RawArtifactMember:
        if isinstance(value, Mapping):
            expected = {
                "member_id",
                "role",
                "upstream_record_id",
                "payload",
                "media_type",
                "parser_version",
            }
            if set(value) != expected:
                raise ValueError(
                    "raw artifact membermapping heeft onverwachte velden"
                )
            value = RawArtifactMember(**{
                key: value[key]
                for key in (
                    "member_id",
                    "role",
                    "upstream_record_id",
                    "payload",
                    "media_type",
                    "parser_version",
                )
            })
        if not isinstance(value, RawArtifactMember):
            raise TypeError(
                "members moeten RawArtifactMember of exacte mappings zijn"
            )
        payload = value.payload
        if not isinstance(payload, bytes):
            raise TypeError("raw artifact memberpayload moet exact bytes zijn")
        return RawArtifactMember(
            member_id=cls._identifier(value.member_id, "member_id", 512),
            role=cls._identifier(value.role, "role", 100),
            upstream_record_id=cls._identifier(
                value.upstream_record_id,
                "upstream_record_id",
                512,
            ),
            payload=payload,
            media_type=cls._identifier(
                value.media_type,
                "media_type",
                200,
            ),
            parser_version=cls._identifier(
                value.parser_version,
                "parser_version",
                200,
            ),
        )

    @staticmethod
    def _timestamps(
        fetched_at: float,
        nominal_at: float | None,
    ) -> tuple[float, float | None]:
        fetched = float(fetched_at)
        nominal = float(nominal_at) if nominal_at is not None else None
        if not math.isfinite(fetched) or fetched < 0:
            raise ValueError("fetched_at moet eindig en niet-negatief zijn")
        if nominal is not None and (
            not math.isfinite(nominal) or nominal < 0
        ):
            raise ValueError(
                "nominal_at moet eindig, niet-negatief of None zijn"
            )
        return fetched, nominal

    @staticmethod
    def _sha256(value: str, name: str) -> str:
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError(f"{name} moet lowercase SHA-256 zijn")
        return value

    @staticmethod
    def _manifest_sha256(members: tuple[RawArtifactMember, ...]) -> str:
        manifest = {
            "schema": RAW_ARTIFACT_SET_MANIFEST_SCHEMA,
            "members": [
                {
                    "member_id": member.member_id,
                    "role": member.role,
                    "upstream_record_id": member.upstream_record_id,
                    "content_sha256": hashlib.sha256(
                        member.payload
                    ).hexdigest(),
                    "size_bytes": len(member.payload),
                    "media_type": member.media_type,
                    "parser_version": member.parser_version,
                }
                for member in members
            ],
        }
        encoded = json.dumps(
            manifest,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _artifact_set_id(provider_id: str, upstream_set_id: str) -> str:
        digest = hashlib.sha256(
            f"{provider_id}\0{upstream_set_id}".encode("utf-8")
        ).hexdigest()
        return f"raw-set-{digest}"

    def _decode_set_receipt(
        self,
        db: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        verify_payloads: bool,
    ) -> RawArtifactSetReceipt:
        member_rows = db.execute(
            """
            SELECT
                member.member_id,
                member.role,
                receipt.receipt_id,
                receipt.provider_id,
                receipt.upstream_record_id,
                receipt.content_sha256,
                receipt.receipt_sha256,
                receipt.fetched_at,
                receipt.nominal_at,
                receipt.media_type,
                receipt.parser_version,
                blob.size_bytes,
                blob.payload
            FROM raw_artifact_set_members AS member
            JOIN raw_artifact_receipts AS receipt
              ON receipt.receipt_id=member.receipt_id
            JOIN raw_artifact_blobs AS blob
              ON blob.content_sha256=receipt.content_sha256
            WHERE member.artifact_set_id=?
            ORDER BY member.member_id
            """,
            (str(row["artifact_set_id"]),),
        ).fetchall()
        provider_id = str(row["provider_id"])
        nominal_at = (
            float(row["nominal_at"])
            if row["nominal_at"] is not None
            else None
        )
        prepared: list[RawArtifactMember] = []
        member_receipts: list[RawArtifactSetMemberReceipt] = []
        for member_row in member_rows:
            member_nominal = (
                float(member_row["nominal_at"])
                if member_row["nominal_at"] is not None
                else None
            )
            if (
                str(member_row["provider_id"]) != provider_id
                or member_nominal != nominal_at
            ):
                raise RawArtifactConflictError(
                    "artifact set bevat receipt met andere provenance"
                )
            if verify_payloads:
                self._verified_receipt(member_row)
            elif not self._receipt_matches(member_row):
                raise RawArtifactConflictError(
                    "artifact set bevat receipt met ongeldige provenance"
                )
            payload = bytes(member_row["payload"])
            prepared.append(RawArtifactMember(
                member_id=str(member_row["member_id"]),
                role=str(member_row["role"]),
                upstream_record_id=str(member_row["upstream_record_id"]),
                payload=payload,
                media_type=str(member_row["media_type"]),
                parser_version=str(member_row["parser_version"]),
            ))
            member_receipts.append(RawArtifactSetMemberReceipt(
                member_id=str(member_row["member_id"]),
                role=str(member_row["role"]),
                receipt_id=str(member_row["receipt_id"]),
                upstream_record_id=str(member_row["upstream_record_id"]),
                content_sha256=str(member_row["content_sha256"]),
                receipt_sha256=str(member_row["receipt_sha256"]),
                size_bytes=int(member_row["size_bytes"]),
                media_type=str(member_row["media_type"]),
                parser_version=str(member_row["parser_version"]),
            ))

        expected_count = int(row["member_count"])
        expected_size = int(row["total_size_bytes"])
        expected_set_receipt_sha256 = self._set_receipt_sha256(
            provider_id=provider_id,
            upstream_set_id=str(row["upstream_set_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            provenance_sha256=str(row["provenance_sha256"]),
            member_count=expected_count,
            total_size_bytes=expected_size,
            fetched_at=float(row["fetched_at"]),
            nominal_at=nominal_at,
        )
        if (
            len(prepared) != expected_count
            or sum(len(member.payload) for member in prepared) != expected_size
            or self._manifest_sha256(tuple(prepared))
            != str(row["manifest_sha256"])
            or self._artifact_set_id(
                provider_id,
                str(row["upstream_set_id"]),
            )
            != str(row["artifact_set_id"])
            or expected_set_receipt_sha256
            != str(row["set_receipt_sha256"])
        ):
            raise RawArtifactConflictError(
                "artifact set faalt membership-, samenvattings- of "
                "manifestverificatie"
            )
        self._sha256(str(row["provenance_sha256"]), "provenance_sha256")
        return RawArtifactSetReceipt(
            artifact_set_id=str(row["artifact_set_id"]),
            provider_id=provider_id,
            upstream_set_id=str(row["upstream_set_id"]),
            manifest_sha256=str(row["manifest_sha256"]),
            provenance_sha256=str(row["provenance_sha256"]),
            set_receipt_sha256=str(row["set_receipt_sha256"]),
            member_count=expected_count,
            total_size_bytes=expected_size,
            fetched_at=float(row["fetched_at"]),
            nominal_at=nominal_at,
            created_at=float(row["created_at"]),
            members=tuple(member_receipts),
        )

    def _assert_same_set(
        self,
        receipt: RawArtifactSetReceipt,
        *,
        manifest_sha256: str,
        provenance_sha256: str,
        nominal_at: float | None,
        members: tuple[RawArtifactMember, ...],
    ) -> None:
        stored_by_member_id = {
            member.member_id: member for member in receipt.members
        }
        expected = tuple(
            RawArtifactSetMemberReceipt(
                member_id=member.member_id,
                role=member.role,
                receipt_id=self._receipt_id(
                    receipt.provider_id,
                    member.upstream_record_id,
                ),
                upstream_record_id=member.upstream_record_id,
                content_sha256=hashlib.sha256(member.payload).hexdigest(),
                receipt_sha256=(
                    stored_by_member_id[member.member_id].receipt_sha256
                    if member.member_id in stored_by_member_id
                    else ""
                ),
                size_bytes=len(member.payload),
                media_type=member.media_type,
                parser_version=member.parser_version,
            )
            for member in members
        )
        if (
            receipt.manifest_sha256 != manifest_sha256
            or receipt.provenance_sha256 != provenance_sha256
            or receipt.nominal_at != nominal_at
            or receipt.members != expected
        ):
            raise RawArtifactConflictError(
                "upstream set-ID werd voor andere immutable members "
                "of provenance hergebruikt"
            )

    @staticmethod
    def _identifier(value: str, name: str, maximum: int) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} moet een niet-lege string zijn")
        normalized = value.strip()
        if len(normalized) > maximum:
            raise ValueError(f"{name} is langer dan {maximum} tekens")
        if "\0" in normalized:
            raise ValueError(f"{name} mag geen NUL-teken bevatten")
        return normalized

    @staticmethod
    def _receipt_sha256(
        *,
        provider_id: str,
        upstream_record_id: str,
        content_sha256: str,
        fetched_at: float,
        nominal_at: float | None,
        media_type: str,
        parser_version: str,
    ) -> str:
        return RawArtifactStore._canonical_sha256({
            "schema": RAW_ARTIFACT_RECEIPT_SCHEMA,
            "provider_id": provider_id,
            "upstream_record_id": upstream_record_id,
            "content_sha256": content_sha256,
            "fetched_at": fetched_at,
            "nominal_at": nominal_at,
            "media_type": media_type,
            "parser_version": parser_version,
        })

    @staticmethod
    def _set_receipt_sha256(
        *,
        provider_id: str,
        upstream_set_id: str,
        manifest_sha256: str,
        provenance_sha256: str,
        member_count: int,
        total_size_bytes: int,
        fetched_at: float,
        nominal_at: float | None,
    ) -> str:
        return RawArtifactStore._canonical_sha256({
            "schema": RAW_ARTIFACT_SET_RECEIPT_SCHEMA,
            "provider_id": provider_id,
            "upstream_set_id": upstream_set_id,
            "manifest_sha256": manifest_sha256,
            "provenance_sha256": provenance_sha256,
            "member_count": member_count,
            "total_size_bytes": total_size_bytes,
            "fetched_at": fetched_at,
            "nominal_at": nominal_at,
        })

    @staticmethod
    def _canonical_sha256(value: Mapping[str, Any]) -> str:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _receipt_id(provider_id: str, upstream_record_id: str) -> str:
        digest = hashlib.sha256(
            f"{provider_id}\0{upstream_record_id}".encode("utf-8")
        ).hexdigest()
        return f"raw-{digest}"

    @classmethod
    def _decode_receipt(cls, row: sqlite3.Row) -> RawArtifactReceipt:
        if not cls._receipt_matches(row):
            raise RawArtifactConflictError(
                "raw artifact receipt faalt metadata-/provenancechecksum"
            )
        return RawArtifactReceipt(
            receipt_id=str(row["receipt_id"]),
            provider_id=str(row["provider_id"]),
            upstream_record_id=str(row["upstream_record_id"]),
            content_sha256=str(row["content_sha256"]),
            receipt_sha256=str(row["receipt_sha256"]),
            size_bytes=int(row["size_bytes"]),
            fetched_at=float(row["fetched_at"]),
            nominal_at=(
                float(row["nominal_at"])
                if row["nominal_at"] is not None
                else None
            ),
            media_type=str(row["media_type"]),
            parser_version=str(row["parser_version"]),
            created_at=float(row["created_at"]),
        )

    @classmethod
    def _receipt_matches(cls, row: sqlite3.Row) -> bool:
        try:
            provider_id = str(row["provider_id"])
            upstream_record_id = str(row["upstream_record_id"])
            nominal_at = (
                float(row["nominal_at"])
                if row["nominal_at"] is not None
                else None
            )
            return (
                str(row["receipt_id"])
                == cls._receipt_id(provider_id, upstream_record_id)
                and cls._receipt_sha256(
                    provider_id=provider_id,
                    upstream_record_id=upstream_record_id,
                    content_sha256=str(row["content_sha256"]),
                    fetched_at=float(row["fetched_at"]),
                    nominal_at=nominal_at,
                    media_type=str(row["media_type"]),
                    parser_version=str(row["parser_version"]),
                )
                == str(row["receipt_sha256"])
            )
        except (OverflowError, TypeError, ValueError):
            return False

    @staticmethod
    def _canonical_schema_sql(value: Any) -> str:
        if not isinstance(value, str):
            return ""
        return "".join(value.split()).lower()

    @staticmethod
    def _schema_objects(
        db: sqlite3.Connection,
    ) -> frozenset[tuple[str, str, str]]:
        return frozenset(
            (
                str(row["type"]),
                str(row["name"]),
                str(row["tbl_name"]),
            )
            for row in db.execute(
                """
                SELECT type, name, tbl_name
                FROM sqlite_master
                WHERE name NOT LIKE 'sqlite_%'
                """
            ).fetchall()
        )

    @staticmethod
    def _blob_matches(row: sqlite3.Row) -> bool:
        try:
            payload = bytes(row["payload"])
            return (
                len(payload) == int(row["size_bytes"])
                and hashlib.sha256(payload).hexdigest()
                == str(row["content_sha256"])
            )
        except (OverflowError, TypeError, ValueError):
            return False

    @classmethod
    def _verified_payload(cls, row: sqlite3.Row) -> bytes:
        if not cls._blob_matches(row):
            raise RawArtifactConflictError(
                "opgeslagen raw artifact faalt lengte- of SHA-256-verificatie"
            )
        return bytes(row["payload"])

    @classmethod
    def _verified_receipt(cls, row: sqlite3.Row) -> bytes:
        payload = cls._verified_payload(row)
        if not cls._receipt_matches(row):
            raise RawArtifactConflictError(
                "raw artifact receipt faalt metadata-/provenancechecksum"
            )
        return payload

    def _integrity_report(self, db: sqlite3.Connection) -> dict:
        messages = [
            str(row[0])
            for row in db.execute("PRAGMA integrity_check").fetchall()
        ]
        foreign_key_violations = len(
            db.execute("PRAGMA foreign_key_check").fetchall()
        )
        schema_object_mismatches = len(
            self._schema_objects(db) ^ self._EXPECTED_SCHEMA_OBJECTS
        )
        missing_blobs = int(
            db.execute(
                """
                SELECT count(*)
                FROM raw_artifact_receipts AS receipt
                LEFT JOIN raw_artifact_blobs AS blob USING(content_sha256)
                WHERE blob.content_sha256 IS NULL
                """
            ).fetchone()[0]
        )
        content_mismatches = sum(
            not self._blob_matches(row)
            for row in db.execute(
                """
                SELECT content_sha256, payload, size_bytes
                FROM raw_artifact_blobs
                """
            ).fetchall()
        )
        receipt_mismatches = sum(
            not self._receipt_matches(row)
            for row in db.execute(
                "SELECT * FROM raw_artifact_receipts"
            ).fetchall()
        )
        set_mismatches = 0
        for row in db.execute(
            "SELECT * FROM raw_artifact_sets ORDER BY artifact_set_id"
        ).fetchall():
            try:
                self._decode_set_receipt(
                    db,
                    row,
                    verify_payloads=True,
                )
            except (RawArtifactConflictError, TypeError, ValueError):
                set_mismatches += 1
        return {
            "healthy": (
                messages == ["ok"]
                and foreign_key_violations == 0
                and schema_object_mismatches == 0
                and missing_blobs == 0
                and content_mismatches == 0
                and receipt_mismatches == 0
                and set_mismatches == 0
            ),
            "sqlite": messages,
            "foreign_key_violations": foreign_key_violations,
            "schema_object_mismatches": schema_object_mismatches,
            "missing_blobs": missing_blobs,
            "content_mismatches": content_mismatches,
            "receipt_mismatches": receipt_mismatches,
            "set_mismatches": set_mismatches,
        }

    @staticmethod
    def _stats(db: sqlite3.Connection) -> dict:
        row = db.execute(
            """
            SELECT
                (SELECT count(*) FROM raw_artifact_blobs),
                (SELECT count(*) FROM raw_artifact_receipts),
                (SELECT count(*) FROM raw_artifact_sets),
                (SELECT coalesce(sum(size_bytes), 0)
                   FROM raw_artifact_blobs)
            """
        ).fetchone()
        return {
            "schema_version": RAW_ARTIFACT_SCHEMA_VERSION,
            "blob_count": int(row[0]),
            "receipt_count": int(row[1]),
            "artifact_set_count": int(row[2]),
            "stored_bytes": int(row[3]),
        }

    def _require_initialized(self) -> None:
        if not self._initialized:
            raise RuntimeError("RawArtifactStore moet eerst initialize() uitvoeren")

    def _checkpoint(self, name: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(name)

    def _connect(self) -> sqlite3.Connection:
        db = self._connect_unconfigured()
        try:
            self._configure_durable_writes(db)
            return db
        except BaseException:
            db.close()
            raise

    def _connect_unconfigured(self) -> sqlite3.Connection:
        db = sqlite3.connect(
            self.path,
            timeout=20,
            isolation_level=None,
        )
        try:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            db.execute("PRAGMA busy_timeout=20000")
            return db
        except BaseException:
            db.close()
            raise

    @staticmethod
    def _configure_durable_writes(db: sqlite3.Connection) -> None:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=FULL")

    @staticmethod
    def _retryable_initialize_error(exc: BaseException) -> bool:
        if not isinstance(exc, sqlite3.OperationalError):
            return False
        message = str(exc).lower()
        return (
            "database is locked" in message
            or "database table is locked" in message
            or "already exists" in message
        )
