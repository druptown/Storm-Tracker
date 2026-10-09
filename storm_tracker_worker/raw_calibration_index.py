"""Durable exact-raw provenance index for the calibration pipeline.

The raw artifact store remains the sole owner of upstream payload bytes.  This
small SQLite database records which verified raw capture belongs to which
worker source so later calibration/replay jobs can discover evidence without
copying large provider products into a second database.
"""
from __future__ import annotations

from contextlib import closing
import math
from pathlib import Path
import re
import sqlite3
from typing import Any

from .raw_artifacts import RawArtifactCaptureReceipt


CONTRACT = "stv3-worker-raw-calibration-index-v1"
SCHEMA_VERSION = 1
MAX_INDEX_ENTRIES = 250_000
MAX_DATABASE_BYTES = 256 * 1024 * 1024
MAX_DIAGNOSTIC_GROUPS = 64
MAX_PROJECTION_BATCH = 128
MAX_PROJECTION_ERROR_CODE = 96

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_LEASE_OWNER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,95}$")
_ERROR_CODE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,95}$")


class RawCalibrationIndexError(RuntimeError):
    """A verified raw capture cannot be represented by the index."""


class RawCalibrationIndex:
    """Append-only, idempotent index over verified raw capture receipts."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _connect(self) -> sqlite3.Connection:
        database = sqlite3.connect(self.path, timeout=5.0)
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA foreign_keys=ON")
        database.execute("PRAGMA busy_timeout=5000")
        return database

    def initialize(self) -> dict[str, Any]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as database:
            database.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_calibration_manifest(
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                ) WITHOUT ROWID;
                CREATE TABLE IF NOT EXISTS raw_calibration_captures(
                    capture_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    product TEXT NOT NULL,
                    capture_receipt_sha256 TEXT NOT NULL
                        CHECK(length(capture_receipt_sha256)=64),
                    manifest_sha256 TEXT NOT NULL
                        CHECK(length(manifest_sha256)=64),
                    product_timestamp REAL,
                    received_at REAL NOT NULL,
                    raw_store_created_at REAL NOT NULL,
                    indexed_at REAL NOT NULL,
                    member_count INTEGER NOT NULL CHECK(member_count>0)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_raw_calibration_source_time
                    ON raw_calibration_captures(source_id, received_at DESC);
                CREATE INDEX IF NOT EXISTS idx_raw_calibration_provider_time
                    ON raw_calibration_captures(provider, received_at DESC);
                CREATE TABLE IF NOT EXISTS raw_calibration_projection_outbox(
                    capture_id TEXT PRIMARY KEY REFERENCES
                        raw_calibration_captures(capture_id) ON DELETE RESTRICT,
                    state TEXT NOT NULL DEFAULT 'ready'
                        CHECK(state IN ('ready','leased','projected')),
                    attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0),
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    lease_owner TEXT,
                    lease_until REAL,
                    projected_at REAL,
                    last_error_code TEXT,
                    CHECK(
                        (state='leased' AND lease_owner IS NOT NULL
                            AND lease_until IS NOT NULL)
                        OR
                        (state!='leased' AND lease_owner IS NULL
                            AND lease_until IS NULL)
                    ),
                    CHECK(
                        (state='projected' AND projected_at IS NOT NULL)
                        OR
                        (state!='projected' AND projected_at IS NULL)
                    )
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS idx_raw_calibration_projection_ready
                    ON raw_calibration_projection_outbox(
                        state,next_attempt_at,capture_id
                    );
                """
            )
            expected = {
                "contract": CONTRACT,
                "schema_version": str(SCHEMA_VERSION),
                "payload_authority": "raw_artifact_store",
            }
            current = dict(
                database.execute(
                    "SELECT key,value FROM raw_calibration_manifest"
                )
            )
            if not current:
                database.executemany(
                    "INSERT INTO raw_calibration_manifest(key,value) VALUES(?,?)",
                    sorted(expected.items()),
                )
            elif current != expected:
                raise RawCalibrationIndexError(
                    "raw calibration index heeft onverenigbaar manifest"
                )
            # Existing v1 indexes are upgraded additively.  The raw capture
            # row remains authoritative; this outbox is only a recoverable
            # asynchronous projection into PostgreSQL/PostGIS.
            database.execute(
                "INSERT OR IGNORE INTO raw_calibration_projection_outbox("
                "capture_id,state,attempts,next_attempt_at) "
                "SELECT capture_id,'ready',0,0 FROM raw_calibration_captures"
            )
            database.commit()
        self._enforce_file_bound()
        return self.statistics()

    @staticmethod
    def _validated_row(
        source_id: str,
        capture: RawArtifactCaptureReceipt,
        indexed_at: float,
    ) -> tuple[Any, ...]:
        if _SOURCE_RE.fullmatch(source_id) is None:
            raise RawCalibrationIndexError("raw calibration source_id is ongeldig")
        if not isinstance(capture, RawArtifactCaptureReceipt) or not capture.complete:
            raise RawCalibrationIndexError("raw calibration capture is onvolledig")
        if (
            not isinstance(capture.capture_id, str)
            or not 0 < len(capture.capture_id) <= 96
            or "\x00" in capture.capture_id
            or _SHA256_RE.fullmatch(capture.capture_receipt_sha256) is None
            or _SHA256_RE.fullmatch(capture.manifest_sha256) is None
        ):
            raise RawCalibrationIndexError(
                "raw calibration capture mist geldige controlesommen"
            )
        if not capture.members:
            raise RawCalibrationIndexError("raw calibration capture heeft geen leden")
        if not isinstance(capture.provider, str) or not 0 < len(capture.provider) <= 128:
            raise RawCalibrationIndexError("raw calibration provider is ongeldig")
        if not isinstance(capture.product, str) or not 0 < len(capture.product) <= 256:
            raise RawCalibrationIndexError("raw calibration product is ongeldig")
        received_at = max(float(member.received_at) for member in capture.members)
        product_timestamp = capture.product_timestamp
        if product_timestamp is not None:
            product_timestamp = float(product_timestamp)
            if not math.isfinite(product_timestamp) or product_timestamp < 0.0:
                raise RawCalibrationIndexError(
                    "raw calibration product_timestamp is ongeldig"
                )
        for value, label in (
            (received_at, "received_at"),
            (float(capture.created_at), "created_at"),
            (float(indexed_at), "indexed_at"),
        ):
            if not math.isfinite(value) or value < 0.0:
                raise RawCalibrationIndexError(
                    f"raw calibration {label} is ongeldig"
                )
        return (
            capture.capture_id,
            source_id,
            capture.provider,
            capture.product,
            capture.capture_receipt_sha256,
            capture.manifest_sha256,
            product_timestamp,
            received_at,
            float(capture.created_at),
            float(indexed_at),
            len(capture.members),
        )

    def record(
        self,
        source_id: str,
        capture: RawArtifactCaptureReceipt,
        *,
        indexed_at: float,
    ) -> dict[str, Any]:
        """Index one already verified raw receipt without reading its bytes."""

        row = self._validated_row(source_id, capture, indexed_at)
        self.initialize()
        with closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                existing = database.execute(
                    "SELECT capture_id,source_id,provider,product,"
                    "capture_receipt_sha256,manifest_sha256,product_timestamp,"
                    "received_at,raw_store_created_at,indexed_at,member_count "
                    "FROM raw_calibration_captures WHERE capture_id=?",
                    (capture.capture_id,),
                ).fetchone()
                if existing is not None:
                    stored = tuple(existing)
                    # indexed_at records the first successful binding and is
                    # intentionally excluded from idempotent receipt equality.
                    if stored[:9] + stored[10:] != row[:9] + row[10:]:
                        raise RawCalibrationIndexError(
                            "raw calibration capture-ID botst met andere provenance"
                        )
                    database.execute(
                        "INSERT OR IGNORE INTO raw_calibration_projection_outbox("
                        "capture_id,state,attempts,next_attempt_at) "
                        "VALUES(?,'ready',0,0)",
                        (capture.capture_id,),
                    )
                    database.commit()
                    result = self._statistics(database)
                    result["already_indexed"] = True
                    return result
                count = int(
                    database.execute(
                        "SELECT count(*) FROM raw_calibration_captures"
                    ).fetchone()[0]
                )
                if count >= MAX_INDEX_ENTRIES:
                    raise RawCalibrationIndexError(
                        "raw calibration index heeft zijn rijgrens bereikt"
                    )
                database.execute(
                    "INSERT INTO raw_calibration_captures("
                    "capture_id,source_id,provider,product,"
                    "capture_receipt_sha256,manifest_sha256,product_timestamp,"
                    "received_at,raw_store_created_at,indexed_at,member_count"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    row,
                )
                database.execute(
                    "INSERT INTO raw_calibration_projection_outbox("
                    "capture_id,state,attempts,next_attempt_at) "
                    "VALUES(?,'ready',0,0)",
                    (capture.capture_id,),
                )
                database.commit()
            except BaseException:
                database.rollback()
                raise
        self._enforce_file_bound()
        result = self.statistics()
        result["already_indexed"] = False
        return result

    def _enforce_file_bound(self) -> None:
        if self.path.exists() and self.path.stat().st_size > MAX_DATABASE_BYTES:
            raise RawCalibrationIndexError(
                "raw calibration index overschrijdt het bytebudget"
            )

    def _statistics(self, database: sqlite3.Connection) -> dict[str, Any]:
        row = database.execute(
            "SELECT count(*) AS captures,count(DISTINCT source_id) AS sources,"
            "count(DISTINCT provider) AS providers,max(received_at) AS latest "
            "FROM raw_calibration_captures"
        ).fetchone()
        projection_rows = dict(
            database.execute(
                "SELECT state,count(*) FROM raw_calibration_projection_outbox "
                "GROUP BY state"
            )
        )
        return {
            "contract": CONTRACT,
            "status": "ready",
            "captures": int(row["captures"]),
            "sources": int(row["sources"]),
            "providers": int(row["providers"]),
            "latest_received_at": row["latest"],
            "database_bytes": self.path.stat().st_size if self.path.exists() else 0,
            "payload_storage": "raw_artifact_store",
            "exact_raw": True,
            "postgis_projection": {
                "ready": int(projection_rows.get("ready", 0)),
                "leased": int(projection_rows.get("leased", 0)),
                "projected": int(projection_rows.get("projected", 0)),
            },
        }

    def statistics(self) -> dict[str, Any]:
        if not self.path.exists():
            return {
                "contract": CONTRACT,
                "status": "not_initialized",
                "captures": 0,
                "sources": 0,
                "providers": 0,
                "latest_received_at": None,
                "database_bytes": 0,
                "payload_storage": "raw_artifact_store",
                "exact_raw": True,
                "postgis_projection": {
                    "ready": 0,
                    "leased": 0,
                    "projected": 0,
                },
            }
        with closing(self._connect()) as database:
            result = self._statistics(database)
        self._enforce_file_bound()
        return result

    def source_statistics(self) -> tuple[dict[str, Any], ...]:
        """Return a bounded read-only provider/source inventory for audits."""

        if not self.path.exists():
            return ()
        with closing(self._connect()) as database:
            rows = database.execute(
                "SELECT source_id,provider,count(*) AS captures,"
                "max(received_at) AS latest_received_at "
                "FROM raw_calibration_captures GROUP BY source_id,provider "
                "ORDER BY source_id,provider LIMIT ?",
                (MAX_DIAGNOSTIC_GROUPS + 1,),
            ).fetchall()
        if len(rows) > MAX_DIAGNOSTIC_GROUPS:
            raise RawCalibrationIndexError(
                "raw calibration bronnensamenvatting overschrijdt haar grens"
            )
        return tuple(dict(row) for row in rows)

    @staticmethod
    def _validated_projection_request(
        lease_owner: str,
        now: float,
    ) -> tuple[str, float]:
        if _LEASE_OWNER_RE.fullmatch(lease_owner) is None:
            raise RawCalibrationIndexError(
                "raw calibration projectie lease_owner is ongeldig"
            )
        moment = float(now)
        if not math.isfinite(moment) or moment < 0.0:
            raise RawCalibrationIndexError(
                "raw calibration projectieklok is ongeldig"
            )
        return lease_owner, moment

    def claim_projection_batch(
        self,
        *,
        lease_owner: str,
        now: float,
        limit: int = 64,
        lease_seconds: float = 120.0,
    ) -> tuple[dict[str, Any], ...]:
        """Lease a bounded provenance batch for asynchronous PostGIS ingest."""

        owner, moment = self._validated_projection_request(lease_owner, now)
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise RawCalibrationIndexError(
                "raw calibration projectiebatch is ongeldig"
            )
        if not 1 <= limit <= MAX_PROJECTION_BATCH:
            raise RawCalibrationIndexError(
                "raw calibration projectiebatch overschrijdt haar grens"
            )
        lease_duration = float(lease_seconds)
        if not math.isfinite(lease_duration) or not 5.0 <= lease_duration <= 900.0:
            raise RawCalibrationIndexError(
                "raw calibration projectielease is ongeldig"
            )
        self.initialize()
        with closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                database.execute(
                    "UPDATE raw_calibration_projection_outbox SET "
                    "state='ready',lease_owner=NULL,lease_until=NULL," 
                    "next_attempt_at=? WHERE state='leased' AND lease_until<=?",
                    (moment, moment),
                )
                rows = database.execute(
                    "SELECT c.capture_id,c.source_id,c.provider,c.product,"
                    "c.capture_receipt_sha256,c.manifest_sha256,"
                    "c.product_timestamp,c.received_at,c.raw_store_created_at,"
                    "c.indexed_at,c.member_count,o.attempts "
                    "FROM raw_calibration_projection_outbox AS o JOIN "
                    "raw_calibration_captures AS c USING(capture_id) "
                    "WHERE o.state='ready' AND o.next_attempt_at<=? "
                    "ORDER BY c.indexed_at,c.capture_id LIMIT ?",
                    (moment, limit),
                ).fetchall()
                capture_ids = [str(row["capture_id"]) for row in rows]
                for capture_id in capture_ids:
                    updated = database.execute(
                        "UPDATE raw_calibration_projection_outbox SET "
                        "state='leased',attempts=attempts+1,lease_owner=?,"
                        "lease_until=?,last_error_code=NULL "
                        "WHERE capture_id=? AND state='ready'",
                        (owner, moment + lease_duration, capture_id),
                    )
                    if updated.rowcount != 1:
                        raise RawCalibrationIndexError(
                            "raw calibration projectielease botst"
                        )
                database.commit()
            except BaseException:
                database.rollback()
                raise
        return tuple(
            {**dict(row), "attempts": int(row["attempts"]) + 1}
            for row in rows
        )

    def expedite_projection_retries(self, *, now: float) -> int:
        """Make retained ready rows immediately eligible after operator restart.

        The durable attempt counter is intentionally retained.  This only
        removes an obsolete wall-clock delay after an external database repair;
        leased and already projected rows remain untouched.
        """

        _owner, moment = self._validated_projection_request(
            "postgis-startup-recovery", now
        )
        self.initialize()
        with closing(self._connect()) as database:
            updated = database.execute(
                "UPDATE raw_calibration_projection_outbox SET "
                "next_attempt_at=? WHERE state='ready' AND next_attempt_at>?",
                (moment, moment),
            )
            database.commit()
            return int(updated.rowcount)

    @staticmethod
    def _validated_capture_ids(capture_ids: Any) -> tuple[str, ...]:
        if not isinstance(capture_ids, (tuple, list)) or not capture_ids:
            raise RawCalibrationIndexError(
                "raw calibration projectiecapturelijst is ongeldig"
            )
        result = tuple(capture_ids)
        if len(result) > MAX_PROJECTION_BATCH or len(set(result)) != len(result):
            raise RawCalibrationIndexError(
                "raw calibration projectiecapturelijst overschrijdt haar grens"
            )
        if any(
            not isinstance(value, str)
            or not 0 < len(value) <= 96
            or "\x00" in value
            for value in result
        ):
            raise RawCalibrationIndexError(
                "raw calibration projectiecapture-ID is ongeldig"
            )
        return result

    def complete_projection_batch(
        self,
        capture_ids: tuple[str, ...] | list[str],
        *,
        lease_owner: str,
        projected_at: float,
    ) -> None:
        """Acknowledge only after the complete PostgreSQL transaction commits."""

        owner, moment = self._validated_projection_request(
            lease_owner, projected_at
        )
        ids = self._validated_capture_ids(capture_ids)
        with closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                for capture_id in ids:
                    updated = database.execute(
                        "UPDATE raw_calibration_projection_outbox SET "
                        "state='projected',lease_owner=NULL,lease_until=NULL,"
                        "projected_at=?,last_error_code=NULL "
                        "WHERE capture_id=? AND state='leased' "
                        "AND lease_owner=?",
                        (moment, capture_id, owner),
                    )
                    if updated.rowcount != 1:
                        raise RawCalibrationIndexError(
                            "raw calibration projectieack botst"
                        )
                database.commit()
            except BaseException:
                database.rollback()
                raise
        self._enforce_file_bound()

    def retry_projection_batch(
        self,
        capture_ids: tuple[str, ...] | list[str],
        *,
        lease_owner: str,
        now: float,
        delay_seconds: float,
        error_code: str,
    ) -> None:
        """Release a failed batch with bounded backoff and no error payload."""

        owner, moment = self._validated_projection_request(lease_owner, now)
        ids = self._validated_capture_ids(capture_ids)
        delay = float(delay_seconds)
        if not math.isfinite(delay) or not 1.0 <= delay <= 3600.0:
            raise RawCalibrationIndexError(
                "raw calibration projectieretry is ongeldig"
            )
        if (
            not isinstance(error_code, str)
            or len(error_code) > MAX_PROJECTION_ERROR_CODE
            or _ERROR_CODE_RE.fullmatch(error_code) is None
        ):
            raise RawCalibrationIndexError(
                "raw calibration projectiefoutcode is ongeldig"
            )
        with closing(self._connect()) as database:
            database.execute("BEGIN IMMEDIATE")
            try:
                for capture_id in ids:
                    updated = database.execute(
                        "UPDATE raw_calibration_projection_outbox SET "
                        "state='ready',lease_owner=NULL,lease_until=NULL,"
                        "next_attempt_at=?,projected_at=NULL,last_error_code=? "
                        "WHERE capture_id=? AND state='leased' "
                        "AND lease_owner=?",
                        (moment + delay, error_code, capture_id, owner),
                    )
                    if updated.rowcount != 1:
                        raise RawCalibrationIndexError(
                            "raw calibration projectieretry botst"
                        )
                database.commit()
            except BaseException:
                database.rollback()
                raise
        self._enforce_file_bound()


__all__ = [
    "CONTRACT",
    "MAX_DATABASE_BYTES",
    "MAX_INDEX_ENTRIES",
    "MAX_PROJECTION_BATCH",
    "RawCalibrationIndex",
    "RawCalibrationIndexError",
    "SCHEMA_VERSION",
]
