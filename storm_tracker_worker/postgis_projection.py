"""Optional, asynchronous PostgreSQL/PostGIS calibration projection.

The local raw artifact store and RawCalibrationIndex remain authoritative.  A
PostgreSQL outage therefore only grows the durable local outbox; it never turns
an otherwise valid provider capture into a failed worker job.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import math
import os
import secrets
import time
from typing import Any, Callable, Mapping

from .raw_calibration_index import RawCalibrationIndex


CONTRACT = "stv3-postgis-raw-calibration-projection-v1"
PROJECTOR_VERSION = 1
DEFAULT_BATCH_SIZE = 64
ALLOWED_SSL_MODES = frozenset({"disable", "require", "verify-full"})


class PostGISProjectionError(RuntimeError):
    """The optional calibration projection is invalid or inconsistent."""


@dataclass(frozen=True, slots=True)
class PostGISProjectionSettings:
    host: str
    port: int
    database: str
    user: str
    password: str = field(repr=False)
    sslmode: str = "disable"
    connect_timeout: int = 3

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> "PostGISProjectionSettings | None":
        values = os.environ if environment is None else environment
        enabled = str(values.get("STV3_POSTGIS_ENABLED", "false")).strip().lower()
        if enabled in {"", "0", "false", "no", "off"}:
            return None
        if enabled not in {"1", "true", "yes", "on"}:
            raise PostGISProjectionError("postgis_enabled_invalid")
        host = str(values.get("STV3_POSTGIS_HOST", "")).strip()
        database = str(values.get("STV3_POSTGIS_DATABASE", "")).strip()
        user = str(values.get("STV3_POSTGIS_USER", "")).strip()
        password = str(values.get("STV3_POSTGIS_PASSWORD", ""))
        sslmode = str(values.get("STV3_POSTGIS_SSLMODE", "disable")).strip()
        try:
            port = int(values.get("STV3_POSTGIS_PORT", "5432"))
            timeout = int(values.get("STV3_POSTGIS_CONNECT_TIMEOUT", "3"))
        except (TypeError, ValueError) as error:
            raise PostGISProjectionError("postgis_numeric_config_invalid") from error
        if (
            not host
            or len(host) > 253
            or not database
            or len(database) > 63
            or not user
            or len(user) > 63
            or not password
            or len(password) > 1024
            or not 1 <= port <= 65535
            or sslmode not in ALLOWED_SSL_MODES
            or not 1 <= timeout <= 10
        ):
            raise PostGISProjectionError("postgis_config_incomplete")
        return cls(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            sslmode=sslmode,
            connect_timeout=timeout,
        )

    def connect_kwargs(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.database,
            "user": self.user,
            "password": self.password,
            "sslmode": self.sslmode,
            "connect_timeout": self.connect_timeout,
            "application_name": "storm_tracker_worker_postgis_projection",
        }


_INSERT_SQL = """
INSERT INTO stv3_ingest.raw_captures(
    capture_id,source_id,provider,product,capture_receipt_sha256,
    manifest_sha256,product_timestamp,received_at,raw_store_created_at,
    worker_indexed_at,member_count,projector_contract,projector_version
) VALUES(
    %s,%s,%s,%s,%s,%s,
    CASE WHEN %s IS NULL THEN NULL ELSE to_timestamp(%s) END,
    to_timestamp(%s),to_timestamp(%s),to_timestamp(%s),%s,%s,%s
)
ON CONFLICT(capture_id) DO NOTHING
"""

_SELECT_SQL = """
SELECT capture_id,source_id,provider,product,capture_receipt_sha256,
       manifest_sha256,
       CASE WHEN product_timestamp IS NULL THEN NULL
            ELSE extract(epoch FROM product_timestamp)::double precision END,
       extract(epoch FROM received_at)::double precision,
       extract(epoch FROM raw_store_created_at)::double precision,
       extract(epoch FROM worker_indexed_at)::double precision,
       member_count,projector_contract,projector_version
FROM stv3_ingest.raw_captures WHERE capture_id=%s
"""


class PostGISRawCalibrationProjector:
    """Project a bounded outbox batch with idempotent provenance checks."""

    def __init__(
        self,
        index: RawCalibrationIndex,
        settings: PostGISProjectionSettings,
        *,
        clock: Callable[[], float] = time.time,
        connect_factory: Callable[..., Any] | None = None,
        batch_size: int = DEFAULT_BATCH_SIZE,
    ) -> None:
        if not isinstance(index, RawCalibrationIndex):
            raise TypeError("postgis projector index heeft het verkeerde type")
        if not isinstance(settings, PostGISProjectionSettings):
            raise TypeError("postgis projector settings hebben het verkeerde type")
        if not callable(clock) or not 1 <= batch_size <= 128:
            raise PostGISProjectionError("postgis_projector_config_invalid")
        self._index = index
        self._settings = settings
        self._clock = clock
        self._connect_factory = connect_factory
        self._batch_size = batch_size
        self._lease_owner = f"postgis-{os.getpid()}-{secrets.token_hex(6)}"
        self._counts = {
            "flushes": 0,
            "projected": 0,
            "retries": 0,
            "conflicts": 0,
        }
        self._last_success_at: float | None = None
        self._last_error_code: str | None = None

    def _now(self) -> float:
        value = float(self._clock())
        if not math.isfinite(value) or value < 0.0:
            raise PostGISProjectionError("postgis_projection_clock_invalid")
        return value

    def _connect(self):
        factory = self._connect_factory
        if factory is None:
            try:
                import psycopg
            except ImportError as error:
                raise PostGISProjectionError(
                    "postgis_driver_unavailable"
                ) from error
            factory = psycopg.connect
        return factory(**self._settings.connect_kwargs())

    @staticmethod
    def _insert_parameters(row: Mapping[str, Any]) -> tuple[Any, ...]:
        product_timestamp = row["product_timestamp"]
        return (
            row["capture_id"],
            row["source_id"],
            row["provider"],
            row["product"],
            row["capture_receipt_sha256"],
            row["manifest_sha256"],
            product_timestamp,
            product_timestamp,
            row["received_at"],
            row["raw_store_created_at"],
            row["indexed_at"],
            row["member_count"],
            CONTRACT,
            PROJECTOR_VERSION,
        )

    @staticmethod
    def _matches(row: Mapping[str, Any], stored: tuple[Any, ...]) -> bool:
        expected_text = (
            row["capture_id"],
            row["source_id"],
            row["provider"],
            row["product"],
            row["capture_receipt_sha256"],
            row["manifest_sha256"],
        )
        if tuple(stored[:6]) != expected_text:
            return False
        expected_times = (
            row["product_timestamp"],
            row["received_at"],
            row["raw_store_created_at"],
            row["indexed_at"],
        )
        for expected, actual in zip(expected_times, stored[6:10]):
            if expected is None or actual is None:
                if expected is not actual:
                    return False
            elif not math.isclose(
                float(expected), float(actual), rel_tol=0.0, abs_tol=0.000002
            ):
                return False
        return (
            int(stored[10]) == int(row["member_count"])
            and stored[11] == CONTRACT
            and int(stored[12]) == PROJECTOR_VERSION
        )

    def _status(self, status: str, batch: int = 0) -> dict[str, Any]:
        projection = self._index.statistics()["postgis_projection"]
        return {
            "contract": CONTRACT,
            "enabled": True,
            "status": status,
            "batch": batch,
            "ready": projection["ready"],
            "leased": projection["leased"],
            "projected": projection["projected"],
            "last_success_at": self._last_success_at,
            "last_error_code": self._last_error_code,
            "counts": dict(self._counts),
        }

    def flush_once(self) -> dict[str, Any]:
        """Project at most one batch; database errors remain non-critical."""

        now = self._now()
        self._counts["flushes"] += 1
        rows = self._index.claim_projection_batch(
            lease_owner=self._lease_owner,
            now=now,
            limit=self._batch_size,
            lease_seconds=120.0,
        )
        if not rows:
            self._last_error_code = None
            return self._status("current")
        capture_ids = tuple(str(row["capture_id"]) for row in rows)
        try:
            connection = self._connect()
            try:
                with connection.cursor() as cursor:
                    for row in rows:
                        cursor.execute(_INSERT_SQL, self._insert_parameters(row))
                        cursor.execute(_SELECT_SQL, (row["capture_id"],))
                        stored = cursor.fetchone()
                        if stored is None or not self._matches(row, tuple(stored)):
                            self._counts["conflicts"] += 1
                            raise PostGISProjectionError(
                                "postgis_provenance_conflict"
                            )
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
            finally:
                connection.close()
        except Exception as error:
            attempts = max(int(row["attempts"]) for row in rows)
            delay = min(3600.0, max(5.0, float(2 ** min(attempts, 10))))
            code = (
                "postgis_provenance_conflict"
                if isinstance(error, PostGISProjectionError)
                and str(error) == "postgis_provenance_conflict"
                else "postgis_projection_failed"
            )
            self._index.retry_projection_batch(
                capture_ids,
                lease_owner=self._lease_owner,
                now=self._now(),
                delay_seconds=delay,
                error_code=code,
            )
            self._counts["retries"] += len(rows)
            self._last_error_code = code
            return self._status("degraded", len(rows))

        completed_at = self._now()
        self._index.complete_projection_batch(
            capture_ids,
            lease_owner=self._lease_owner,
            projected_at=completed_at,
        )
        self._counts["projected"] += len(rows)
        self._last_success_at = completed_at
        self._last_error_code = None
        return self._status("current", len(rows))


__all__ = [
    "ALLOWED_SSL_MODES",
    "CONTRACT",
    "DEFAULT_BATCH_SIZE",
    "PROJECTOR_VERSION",
    "PostGISProjectionError",
    "PostGISProjectionSettings",
    "PostGISRawCalibrationProjector",
]
