"""Resumable, non-destructive migration of legacy ground observations."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
import json
import math
from typing import Any, Callable, Mapping

from .thin_observation import ObservationQuality
from .thin_postgis import _INSERT_STATION_MEASUREMENT
from .thin_station_catalog import normalize_capabilities


MIGRATION_ID = "legacy_observation_v1"


@dataclass(frozen=True, slots=True)
class BackfillReport:
    status: str
    source_cursor: str | None
    fetched_rows: int
    copied_count: int
    rejected_count: int


@dataclass(frozen=True, slots=True)
class VerificationBucket:
    provider_id: str
    measurement_type: str
    legacy_distinct: int
    canonical_count: int
    minimum_observed_at: float | None
    maximum_observed_at: float | None
    minimum_value: float | None
    maximum_value: float | None
    duplicate_reduction_count: int


@dataclass(frozen=True, slots=True)
class VerificationReport:
    status: str
    buckets: tuple[VerificationBucket, ...]
    unrecognizable_rows: int
    orphan_count: int


_SELECT_STATE = """
SELECT source_cursor,copied_count,rejected_count,status
FROM stv3_station_measurement_backfill_state
WHERE migration_id=%s
FOR UPDATE
"""

_SELECT_LEGACY_BATCH = """
SELECT observation.observation_key,observation.provider_id,
       observation.product_id,observation.value,observation.unit,
       EXTRACT(EPOCH FROM observation.observed_at),
       EXTRACT(EPOCH FROM observation.received_at),
       observation.quality,observation.metadata,
       station.station_id IS NOT NULL
FROM stv3_observation AS observation
LEFT JOIN stv3_ground_station AS station
  ON station.provider_id=observation.provider_id
 AND station.station_id=observation.metadata->>'station_id'
WHERE (%s::text IS NULL OR observation.observation_key>%s)
  AND observation.observed_at >=
      date_trunc('month',CURRENT_TIMESTAMP)-interval '6 months'
  AND NULLIF(observation.metadata->>'station_id','') IS NOT NULL
  AND NULLIF(observation.metadata->>'measurement','') IS NOT NULL
ORDER BY observation.observation_key
LIMIT %s
"""

_UPDATE_STATE = """
UPDATE stv3_station_measurement_backfill_state
SET source_cursor=%s,copied_count=%s,rejected_count=%s,status=%s,
    updated_at=clock_timestamp()
WHERE migration_id=%s
"""

_VERIFY_BUCKETS = """
WITH legacy AS (
    SELECT observation.provider_id,
           lower(observation.metadata->>'measurement') AS measurement_type,
           COUNT(DISTINCT ROW(
               observation.provider_id,
               observation.metadata->>'station_id',
               observation.observed_at,
               lower(observation.metadata->>'measurement')
           )) AS legacy_distinct,
           COUNT(*)-COUNT(DISTINCT ROW(
               observation.provider_id,
               observation.metadata->>'station_id',
               observation.observed_at,
               lower(observation.metadata->>'measurement')
           )) AS duplicate_reduction_count,
           EXTRACT(EPOCH FROM MIN(observation.observed_at)) AS minimum_observed_at,
           EXTRACT(EPOCH FROM MAX(observation.observed_at)) AS maximum_observed_at,
           MIN(observation.value) AS minimum_value,
           MAX(observation.value) AS maximum_value
    FROM stv3_observation AS observation
    WHERE observation.observed_at>=to_timestamp(%s)
      AND observation.observed_at<to_timestamp(%s)
      AND NULLIF(observation.metadata->>'station_id','') IS NOT NULL
      AND NULLIF(observation.metadata->>'measurement','') IS NOT NULL
    GROUP BY observation.provider_id,
             lower(observation.metadata->>'measurement')
), canonical AS (
    SELECT measurement.provider_id,measurement.measurement_type,
           COUNT(*) AS canonical_count
    FROM stv3_station_measurement AS measurement
    WHERE measurement.observed_at>=to_timestamp(%s)
      AND measurement.observed_at<to_timestamp(%s)
    GROUP BY measurement.provider_id,measurement.measurement_type
)
SELECT COALESCE(legacy.provider_id,canonical.provider_id),
       COALESCE(legacy.measurement_type,canonical.measurement_type),
       COALESCE(legacy.legacy_distinct,0) AS legacy_distinct,
       COALESCE(canonical.canonical_count,0) AS canonical_count,
       legacy.minimum_observed_at,legacy.maximum_observed_at,
       legacy.minimum_value,legacy.maximum_value,
       COALESCE(legacy.duplicate_reduction_count,0)
FROM legacy
FULL OUTER JOIN canonical
  ON canonical.provider_id=legacy.provider_id
 AND canonical.measurement_type=legacy.measurement_type
ORDER BY 1,2
"""

_COUNT_UNRECOGNIZABLE = """
SELECT COUNT(*) AS unrecognizable_rows
FROM stv3_observation
WHERE observed_at>=to_timestamp(%s) AND observed_at<to_timestamp(%s)
  AND (
      (metadata ? 'station_id' OR metadata ? 'measurement')
      AND (
          NULLIF(metadata->>'station_id','') IS NULL
          OR NULLIF(metadata->>'measurement','') IS NULL
      )
  )
"""

_COUNT_ORPHANS = """
SELECT COUNT(*) AS orphan_count
FROM stv3_station_measurement AS measurement
LEFT JOIN stv3_ground_station AS station
  ON station.provider_id=measurement.provider_id
 AND station.station_id=measurement.station_id
WHERE measurement.observed_at>=to_timestamp(%s)
  AND measurement.observed_at<to_timestamp(%s)
  AND station.station_id IS NULL
"""


def _decoded_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise ValueError("legacy_metadata_invalid")
    return {str(key): item for key, item in value.items()}


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    result = float(value)
    return result if math.isfinite(result) else None


class StationMeasurementMigrationRepository:
    """PostGIS boundary for additive station backfill and verification."""

    def __init__(
        self,
        connect_factory: Callable[..., Any],
        connect_kwargs: Mapping[str, Any],
    ) -> None:
        self._connect_factory = connect_factory
        self._connect_kwargs = dict(connect_kwargs)

    def backfill_batch(self, *, limit: int) -> BackfillReport:
        if isinstance(limit, bool) or not 1 <= int(limit) <= 100_000:
            raise ValueError("backfill_limit_invalid")
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_SELECT_STATE, (MIGRATION_ID,))
                state = cursor.fetchone()
                if state is None:
                    raise RuntimeError("backfill_state_missing")
                source_cursor = None if state[0] is None else str(state[0])
                copied_total = int(state[1])
                rejected_total = int(state[2])
                cursor.execute(
                    _SELECT_LEGACY_BATCH,
                    (source_cursor, source_cursor, int(limit) + 1),
                )
                fetched = tuple(cursor.fetchall())
                has_more = len(fetched) > int(limit)
                rows = fetched[:int(limit)]
                insert_rows: list[tuple[Any, ...]] = []
                rejected = 0
                for row in rows:
                    try:
                        metadata = _decoded_metadata(row[8])
                        station_id = str(metadata.get("station_id", "")).strip()
                        measurement_type = normalize_capabilities((
                            str(metadata.get("measurement", "")),
                        ))[0]
                        quality = ObservationQuality(str(row[7])).value
                        observed_at = float(row[5])
                        received_at = float(row[6])
                        if (
                            not bool(row[9])
                            or not station_id
                            or not math.isfinite(observed_at)
                            or not math.isfinite(received_at)
                        ):
                            raise ValueError("legacy_station_measurement_invalid")
                        insert_rows.append((
                            str(row[1]),
                            station_id,
                            observed_at,
                            measurement_type,
                            str(row[2]),
                            float(row[3]),
                            str(row[4]),
                            received_at,
                            quality,
                            json.dumps(
                                metadata,
                                sort_keys=True,
                                separators=(",", ":"),
                                allow_nan=False,
                            ),
                        ))
                    except (IndexError, TypeError, ValueError, json.JSONDecodeError):
                        rejected += 1
                if insert_rows:
                    cursor.executemany(_INSERT_STATION_MEASUREMENT, insert_rows)
                next_cursor = source_cursor
                if rows:
                    next_cursor = str(rows[-1][0])
                status = "partial" if has_more else "complete"
                copied_total += len(insert_rows)
                rejected_total += rejected
                cursor.execute(_UPDATE_STATE, (
                    next_cursor,
                    copied_total,
                    rejected_total,
                    status,
                    MIGRATION_ID,
                ))
            connection.commit()
            return BackfillReport(
                status=status,
                source_cursor=next_cursor,
                fetched_rows=len(rows),
                copied_count=copied_total,
                rejected_count=rejected_total,
            )
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def verify(
        self,
        *,
        window_start: float,
        window_end: float,
    ) -> VerificationReport:
        start = float(window_start)
        end = float(window_end)
        if not math.isfinite(start) or not math.isfinite(end) or not start < end:
            raise ValueError("verification_window_invalid")
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_VERIFY_BUCKETS, (start, end, start, end))
                bucket_rows = tuple(cursor.fetchall())
                cursor.execute(_COUNT_UNRECOGNIZABLE, (start, end))
                unrecognizable_row = cursor.fetchone()
                cursor.execute(_COUNT_ORPHANS, (start, end))
                orphan_row = cursor.fetchone()
        finally:
            connection.close()
        buckets = tuple(VerificationBucket(
            provider_id=str(row[0]),
            measurement_type=str(row[1]),
            legacy_distinct=int(row[2]),
            canonical_count=int(row[3]),
            minimum_observed_at=_optional_float(row[4]),
            maximum_observed_at=_optional_float(row[5]),
            minimum_value=_optional_float(row[6]),
            maximum_value=_optional_float(row[7]),
            duplicate_reduction_count=int(row[8]),
        ) for row in bucket_rows)
        unrecognizable = 0 if unrecognizable_row is None else int(unrecognizable_row[0])
        orphans = 0 if orphan_row is None else int(orphan_row[0])
        verified = (
            orphans == 0
            and all(
                bucket.legacy_distinct == bucket.canonical_count
                for bucket in buckets
            )
        )
        return VerificationReport(
            status="verified" if verified else "mismatch",
            buckets=buckets,
            unrecognizable_rows=unrecognizable,
            orphan_count=orphans,
        )

    def dry_run(
        self,
        *,
        window_start: float,
        window_end: float,
    ) -> VerificationReport:
        return replace(
            self.verify(window_start=window_start, window_end=window_end),
            status="ready",
        )


__all__ = [
    "BackfillReport",
    "MIGRATION_ID",
    "StationMeasurementMigrationRepository",
    "VerificationBucket",
    "VerificationReport",
]
