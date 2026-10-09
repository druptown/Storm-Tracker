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

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


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
                    database.rollback()
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


__all__ = [
    "CONTRACT",
    "MAX_DATABASE_BYTES",
    "MAX_INDEX_ENTRIES",
    "RawCalibrationIndex",
    "RawCalibrationIndexError",
    "SCHEMA_VERSION",
]
