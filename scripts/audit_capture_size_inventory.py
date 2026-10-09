"""Read-only size and temporal inventory for calibration frame storage.

This utility deliberately audits the two legacy calibration tables as they
exist.  It does not promote provider overlays to upstream-raw artifacts and it
does not make retention or licensing decisions.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
from statistics import median
import sqlite3
from typing import Any, Iterable, Mapping
import zlib


REPORT_TYPE = "calibration_capture_size_inventory_v1"
READ_ONLY_CONNECTION_MODE = "sqlite-uri-mode-ro-query-only"
OVERLAY_ENCODING = "zlib-json-v1"

DEFAULT_MAX_COMPRESSED_BYTES = 8 * 1024 * 1024
DEFAULT_MAX_DECODED_BYTES = 64 * 1024 * 1024
DEFAULT_MAX_DECOMPRESSION_RATIO = 250.0
DEFAULT_MAX_FRAME_JSON_BYTES = 64 * 1024 * 1024

_RAW_HEADER_KEYS = frozenset({
    "headers",
    "http_headers",
    "raw_headers",
    "response_headers",
    "transport_headers",
})
_QUALITY_KEYS = frozenset({
    "qc",
    "quality",
    "quality_control",
    "quality_flags",
    "quality_mask",
})
_NODATA_KEYS = frozenset({
    "_fillvalue",
    "missing_value",
    "no_data",
    "nodata",
    "nodata_mask",
    "nodata_value",
    "valid_mask",
})

_ORIGINAL_REQUIRED = {
    "region_id",
    "source",
    "nominal_timestamp",
    "saved_at",
    "encoding",
    "payload",
}
_CACHE_REQUIRED = {
    "region_id",
    "source",
    "nominal_timestamp",
    "saved_at",
    "frame_json",
}


class CaptureInventoryError(RuntimeError):
    """Raised when the read-only inventory cannot be completed safely."""


def audit_capture_size_inventory(
    database_path: str | Path,
    *,
    maximum_compressed_bytes: int = DEFAULT_MAX_COMPRESSED_BYTES,
    maximum_decoded_bytes: int = DEFAULT_MAX_DECODED_BYTES,
    maximum_decompression_ratio: float = DEFAULT_MAX_DECOMPRESSION_RATIO,
    maximum_frame_json_bytes: int = DEFAULT_MAX_FRAME_JSON_BYTES,
    writer_source_path: str | Path | None = None,
) -> dict[str, Any]:
    """Inventory surviving frame records without opening a write channel."""

    limits = {
        "maximum_compressed_frame_bytes": _positive_int(
            maximum_compressed_bytes,
            "maximum_compressed_bytes",
        ),
        "maximum_decoded_frame_bytes": _positive_int(
            maximum_decoded_bytes,
            "maximum_decoded_bytes",
        ),
        "maximum_decompression_ratio": _positive_finite(
            maximum_decompression_ratio,
            "maximum_decompression_ratio",
        ),
        "maximum_frame_json_bytes": _positive_int(
            maximum_frame_json_bytes,
            "maximum_frame_json_bytes",
        ),
    }
    path = Path(database_path).expanduser().resolve(strict=True)
    if not path.is_file():
        raise CaptureInventoryError(f"databasepad is geen bestand: {path}")
    sha_before = _sha256(path)
    uri = f"{path.as_uri()}?mode=ro"
    try:
        database = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as err:
        raise CaptureInventoryError(
            f"database kan niet read-only worden geopend: {err}"
        ) from err

    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        if int(database.execute("PRAGMA query_only").fetchone()[0]) != 1:
            raise CaptureInventoryError("SQLite query_only kon niet worden gezet")
        tables = {
            str(row["name"])
            for row in database.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        writer_path = _resolve_writer_source(writer_source_path)
        original = _audit_original_table(
            database,
            present="original_radar_frames" in tables,
            limits=limits,
            writer_path=writer_path,
        )
        cache = _audit_cache_table(
            database,
            present="pysteps_frame_cache" in tables,
            maximum_json_bytes=limits["maximum_frame_json_bytes"],
            writer_path=writer_path,
        )
    except sqlite3.Error as err:
        raise CaptureInventoryError(
            f"read-only SQLite-inventaris mislukt: {err}"
        ) from err
    finally:
        database.close()

    sha_after = _sha256(path)
    report = {
        "report_type": REPORT_TYPE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_path": str(path),
        "database_file_bytes": path.stat().st_size,
        "database_sha256_before": sha_before,
        "database_sha256_after": sha_after,
        "database_unchanged": sha_before == sha_after,
        "connection_mode": READ_ONLY_CONNECTION_MODE,
        "safety_limits": limits,
        "measurement_scope": {
            "stored_bytes": (
                "SQLite length of the payload/blob or UTF-8 JSON only; excludes "
                "table, index, freelist, page, WAL and filesystem overhead"
            ),
            "days": "UTC",
            "cadence": "positive differences between distinct nominal timestamps",
            "gap_rule": (
                "delta > 1.5 times the modal positive cadence; missing frames "
                "estimated as round(delta/cadence)-1"
            ),
            "growth": (
                "surviving payload bytes grouped by current saved_at; this is a "
                "proxy, not observed SQLite file-size growth"
            ),
        },
        "tables": {
            "original_radar_frames": original,
            "pysteps_frame_cache": cache,
        },
        "combined_current_snapshot": {
            "surviving_row_count": (
                original["totals"]["row_count"] + cache["totals"]["row_count"]
            ),
            "stored_payload_bytes": (
                original["totals"]["stored_payload_bytes"]
                + cache["totals"]["stored_payload_bytes"]
            ),
            "database_bytes_are_not_attributed_to_tables": True,
        },
        "limitations": [
            "original_radar_frames stores decoded provider overlays, not exact upstream transport artifacts.",
            "Primary-key upserts can replace an earlier revision at the same region/provider/nominal timestamp.",
            "No append-only size history exists here, so actual historical database growth cannot be reconstructed.",
            "saved_at describes the surviving revision, not every insert or overwrite that occurred.",
            "Top-level JSON field absence cannot prove that a provider never supplied headers, QC or nodata semantics upstream.",
            "Projections are arithmetic scenarios from a short sample, not capacity promises or retention recommendations.",
            "No licensing or retention suitability is evaluated by this report.",
        ],
    }
    return report


def _audit_original_table(
    database: sqlite3.Connection,
    *,
    present: bool,
    limits: Mapping[str, int | float],
    writer_path: Path | None,
) -> dict[str, Any]:
    base = _empty_table(
        semantic_label="provider_overlay_not_upstream_raw",
        present=present,
    )
    if not present:
        base["limitations"].append("original_radar_frames table is absent")
        return base
    columns, primary_key, table_sql = _schema(database, "original_radar_frames")
    missing = sorted(_ORIGINAL_REQUIRED - set(columns))
    base["schema"] = _schema_report(columns, primary_key, table_sql)
    if missing:
        base["limitations"].append(
            "required columns absent: " + ", ".join(missing)
        )
        return base

    records: list[dict[str, Any]] = []
    query = """
        SELECT region_id, source, nominal_timestamp, saved_at, encoding,
               typeof(payload) AS payload_type,
               length(payload) AS stored_bytes,
               CASE WHEN typeof(payload)='blob' AND length(payload) <= ?
                    THEN payload ELSE NULL END AS bounded_payload
          FROM original_radar_frames
         ORDER BY region_id, source, nominal_timestamp
    """
    for row in database.execute(
        query,
        (limits["maximum_compressed_frame_bytes"],),
    ):
        stored_bytes = _nonnegative_int(row["stored_bytes"])
        record = _base_record(row, stored_bytes)
        encoding = str(row["encoding"])
        record["encoding"] = encoding
        record["decoded_bytes"] = None
        record["json_object"] = False
        record["top_level_fields"] = ()
        record["metadata_presence"] = {
            "raw_headers": False,
            "quality_or_qc": False,
            "nodata_or_validity": False,
        }
        if str(row["payload_type"]) != "blob":
            record["decode_status"] = "payload_not_blob"
        elif stored_bytes is None:
            record["decode_status"] = "unknown_payload_length"
        elif stored_bytes > limits["maximum_compressed_frame_bytes"]:
            record["decode_status"] = "compressed_payload_too_large"
        elif encoding != OVERLAY_ENCODING:
            record["decode_status"] = f"unsupported_encoding:{encoding}"
        else:
            payload = row["bounded_payload"]
            try:
                decoded = _limited_zlib_decompress(
                    bytes(payload),
                    maximum_bytes=int(limits["maximum_decoded_frame_bytes"]),
                    maximum_ratio=float(
                        limits["maximum_decompression_ratio"]
                    ),
                )
                record["decoded_bytes"] = len(decoded)
                value = json.loads(decoded.decode("utf-8"))
                if not isinstance(value, dict):
                    record["decode_status"] = "json_not_object"
                else:
                    fields = tuple(sorted(str(key) for key in value))
                    record["decode_status"] = "decoded_json_object"
                    record["json_object"] = True
                    record["top_level_fields"] = fields
                    folded = {field.casefold() for field in fields}
                    record["metadata_presence"] = {
                        "raw_headers": bool(folded & _RAW_HEADER_KEYS),
                        "quality_or_qc": bool(folded & _QUALITY_KEYS),
                        "nodata_or_validity": bool(folded & _NODATA_KEYS),
                    }
            except UnicodeDecodeError:
                record["decode_status"] = "decoded_payload_not_utf8"
            except json.JSONDecodeError:
                record["decode_status"] = "decoded_payload_invalid_json"
            except (TypeError, ValueError, zlib.error) as err:
                record["decode_status"] = str(err) or type(err).__name__
        records.append(record)

    base.update(_record_inventory(records, metadata=True))
    base["storage_semantics"] = _storage_semantics(
        table="original_radar_frames",
        primary_key=primary_key,
        columns=columns,
        writer_path=writer_path,
    )
    base["growth_proxy"] = _growth_proxy(
        records,
        cumulative_projection=True,
    )
    base["limitations"].extend([
        "Decoded byte totals exist only for zlib-json-v1 rows that passed both absolute-size and compression-ratio bounds.",
        "The overlay JSON is already downstream of provider parsing/projection; it cannot reproduce omitted transport bytes.",
        "Metadata checks inspect top-level overlay keys only and therefore report possible absence, not upstream non-existence.",
    ])
    return base


def _audit_cache_table(
    database: sqlite3.Connection,
    *,
    present: bool,
    maximum_json_bytes: int,
    writer_path: Path | None,
) -> dict[str, Any]:
    base = _empty_table(
        semantic_label="normalized_sparse_analysis_cache_not_provider_raw",
        present=present,
    )
    if not present:
        base["limitations"].append("pysteps_frame_cache table is absent")
        return base
    columns, primary_key, table_sql = _schema(database, "pysteps_frame_cache")
    missing = sorted(_CACHE_REQUIRED - set(columns))
    base["schema"] = _schema_report(columns, primary_key, table_sql)
    if missing:
        base["limitations"].append(
            "required columns absent: " + ", ".join(missing)
        )
        return base

    records: list[dict[str, Any]] = []
    query = """
        SELECT region_id, source, nominal_timestamp, saved_at,
               typeof(frame_json) AS frame_type,
               length(CAST(frame_json AS BLOB)) AS stored_bytes,
               CASE WHEN typeof(frame_json)='text'
                          AND length(CAST(frame_json AS BLOB)) <= ?
                    THEN frame_json ELSE NULL END AS bounded_json
          FROM pysteps_frame_cache
         ORDER BY region_id, source, nominal_timestamp
    """
    for row in database.execute(query, (maximum_json_bytes,)):
        stored_bytes = _nonnegative_int(row["stored_bytes"])
        record = _base_record(row, stored_bytes)
        record["decoded_bytes"] = stored_bytes
        record["top_level_fields"] = ()
        if str(row["frame_type"]) != "text":
            record["decode_status"] = "frame_json_not_text"
            record["decoded_bytes"] = None
        elif stored_bytes is None:
            record["decode_status"] = "unknown_frame_json_length"
            record["decoded_bytes"] = None
        elif stored_bytes > maximum_json_bytes:
            record["decode_status"] = "frame_json_too_large"
            record["decoded_bytes"] = None
        else:
            try:
                value = json.loads(str(row["bounded_json"]))
                if isinstance(value, dict):
                    record["decode_status"] = "decoded_json_object"
                    record["top_level_fields"] = tuple(
                        sorted(str(key) for key in value)
                    )
                else:
                    record["decode_status"] = "json_not_object"
            except json.JSONDecodeError:
                record["decode_status"] = "invalid_json"
        records.append(record)

    base.update(_record_inventory(records, metadata=False))
    base["storage_semantics"] = _storage_semantics(
        table="pysteps_frame_cache",
        primary_key=primary_key,
        columns=columns,
        writer_path=writer_path,
    )
    base["growth_proxy"] = _growth_proxy(
        records,
        cumulative_projection=False,
    )
    base["limitations"].extend([
        "frame_json is an uncompressed normalized sparse analysis-grid cache, not provider-native data.",
        "The current snapshot cannot establish historical cache churn or deleted-row volume.",
        "A cumulative long-horizon projection is intentionally not emitted for a replacement/rolling cache.",
    ])
    return base


def _record_inventory(
    records: list[dict[str, Any]],
    *,
    metadata: bool,
) -> dict[str, Any]:
    return {
        "totals": _summarize(records, metadata=metadata),
        "by_provider_region": [
            {
                "provider": source,
                "region_id": region,
                **_summarize(group, metadata=metadata),
            }
            for (source, region), group in _group_records(
                records,
                lambda record: (record["source"], record["region_id"]),
            )
        ],
        "by_provider_region_nominal_day": [
            {
                "provider": source,
                "region_id": region,
                "nominal_utc_day": day,
                **_summarize(group, metadata=metadata),
            }
            for (source, region, day), group in _group_records(
                records,
                lambda record: (
                    record["source"],
                    record["region_id"],
                    _utc_day(record["nominal_timestamp"]) or "invalid",
                ),
            )
        ],
        "by_provider_region_saved_at_day": [
            {
                "provider": source,
                "region_id": region,
                "saved_at_utc_day": day,
                **_summarize(group, metadata=metadata),
                "saved_at_time": _temporal_summary(
                    record["saved_at"] for record in group
                ),
            }
            for (source, region, day), group in _group_records(
                records,
                lambda record: (
                    record["source"],
                    record["region_id"],
                    _utc_day(record["saved_at"]) or "invalid",
                ),
            )
        ],
    }


def _summarize(
    records: list[dict[str, Any]],
    *,
    metadata: bool,
) -> dict[str, Any]:
    statuses = Counter(str(record["decode_status"]) for record in records)
    fields: Counter[str] = Counter()
    for record in records:
        fields.update(record.get("top_level_fields", ()))
    result: dict[str, Any] = {
        "row_count": len(records),
        "stored_payload_bytes": sum(
            record["stored_bytes"] or 0 for record in records
        ),
        "decoded_payload_bytes": sum(
            record["decoded_bytes"] or 0 for record in records
        ),
        "decoded_byte_count_rows": sum(
            record["decoded_bytes"] is not None for record in records
        ),
        "decode_status_counts": dict(sorted(statuses.items())),
        "top_level_json_field_counts": dict(sorted(fields.items())),
        "nominal_time": _temporal_summary(
            record["nominal_timestamp"] for record in records
        ),
    }
    if metadata:
        valid_objects = sum(record.get("json_object", False) for record in records)
        presence = {
            name: sum(
                bool(record.get("metadata_presence", {}).get(name))
                for record in records
            )
            for name in ("raw_headers", "quality_or_qc", "nodata_or_validity")
        }
        result["top_level_metadata_evidence"] = {
            "decoded_json_object_rows": valid_objects,
            "rows_with_explicit_top_level_field": presence,
            "possibly_absent": {
                name: valid_objects > 0 and count == 0
                for name, count in presence.items()
            },
            "interpretation": (
                "possibly_absent means not observed as a recognized top-level "
                "field in safely decoded stored overlays; it is not proof about "
                "the upstream product"
            ),
        }
    return result


def _temporal_summary(values: Iterable[float | None]) -> dict[str, Any]:
    timestamps = sorted({
        float(value)
        for value in values
        if value is not None and math.isfinite(float(value))
    })
    if not timestamps:
        return {
            "unique_timestamp_count": 0,
            "first_timestamp": None,
            "last_timestamp": None,
            "span_seconds": None,
            "cadence_seconds": None,
            "positive_interval_count": 0,
            "gap_count": 0,
            "estimated_missing_frames": 0,
            "largest_gap_seconds": None,
            "largest_gaps": [],
        }
    intervals = [
        current - previous
        for previous, current in zip(timestamps, timestamps[1:])
        if current > previous
    ]
    cadence = _modal_interval(intervals)
    gaps: list[dict[str, Any]] = []
    if cadence is not None and cadence > 0:
        for previous, current in zip(timestamps, timestamps[1:]):
            elapsed = current - previous
            if elapsed > 1.5 * cadence:
                gaps.append({
                    "after": _iso(previous),
                    "before": _iso(current),
                    "elapsed_seconds": _clean_number(elapsed),
                    "estimated_missing_frames": max(
                        0,
                        round(elapsed / cadence) - 1,
                    ),
                })
    largest = sorted(
        gaps,
        key=lambda item: float(item["elapsed_seconds"]),
        reverse=True,
    )[:5]
    return {
        "unique_timestamp_count": len(timestamps),
        "first_timestamp": _iso(timestamps[0]),
        "last_timestamp": _iso(timestamps[-1]),
        "span_seconds": _clean_number(timestamps[-1] - timestamps[0]),
        "cadence_seconds": _clean_number(cadence),
        "median_positive_interval_seconds": (
            _clean_number(median(intervals)) if intervals else None
        ),
        "p95_positive_interval_seconds": _nearest_rank(intervals, 0.95),
        "positive_interval_count": len(intervals),
        "gap_count": len(gaps),
        "estimated_missing_frames": sum(
            int(gap["estimated_missing_frames"]) for gap in gaps
        ),
        "largest_gap_seconds": (
            max(float(gap["elapsed_seconds"]) for gap in gaps)
            if gaps
            else None
        ),
        "largest_gaps": largest,
    }


def _growth_proxy(
    records: list[dict[str, Any]],
    *,
    cumulative_projection: bool,
) -> dict[str, Any]:
    valid = [
        record
        for record in records
        if record["saved_at"] is not None
    ]
    daily: list[dict[str, Any]] = []
    for day, group in _group_records(
        valid,
        lambda record: _utc_day(record["saved_at"]) or "invalid",
    ):
        saved = sorted(float(record["saved_at"]) for record in group)
        stored = sum(record["stored_bytes"] or 0 for record in group)
        decoded = sum(record["decoded_bytes"] or 0 for record in group)
        first_second = datetime.fromtimestamp(saved[0], timezone.utc)
        last_second = datetime.fromtimestamp(saved[-1], timezone.utc)
        boundary_complete = (
            first_second.hour == 0
            and first_second.minute <= 15
            and last_second.hour == 23
            and last_second.minute >= 45
        )
        daily.append({
            "saved_at_utc_day": day,
            "surviving_row_count": len(group),
            "stored_payload_bytes": stored,
            "decoded_payload_bytes": decoded,
            "first_saved_at": _iso(saved[0]),
            "last_saved_at": _iso(saved[-1]),
            "observed_saved_at_span_seconds": _clean_number(saved[-1] - saved[0]),
            "boundary_complete_utc_day": boundary_complete,
        })
    complete = [day for day in daily if day["boundary_complete_utc_day"]]
    basis = complete if complete else daily
    values = [int(day["stored_payload_bytes"]) for day in basis]
    sample_count = len(values)
    uncertainty = (
        "very_high"
        if sample_count < 3
        else "high"
        if sample_count < 7
        else "moderate"
        if sample_count < 30
        else "lower_but_unquantified"
    )
    projection: dict[str, Any]
    if cumulative_projection and values:
        central = float(median(values))
        low = float(min(values))
        high = float(max(values))
        projection = {
            "emitted": True,
            "scenario_not_forecast": True,
            "basis": (
                "boundary-complete saved_at UTC days"
                if complete
                else "all observed saved_at UTC days; boundary days are partial"
            ),
            "basis_day_count": sample_count,
            "sample_uncertainty": uncertainty,
            "uncertainty_is_not_a_confidence_interval": True,
            "observed_range_has_single_value": len(set(values)) == 1,
            "central_daily_stored_payload_bytes": _clean_number(central),
            "observed_daily_range_bytes": [
                _clean_number(low),
                _clean_number(high),
            ],
            "additional_payload_scenarios": {
                str(days): {
                    "days": days,
                    "low_bytes": _clean_number(low * days),
                    "central_bytes": _clean_number(central * days),
                    "high_bytes": _clean_number(high * days),
                }
                for days in (7, 30, 365)
            },
            "excludes": [
                "SQLite page/index/freelist/WAL/filesystem overhead",
                "overwritten revision bytes and write amplification",
                "future provider, region, cadence, payload and availability changes",
            ],
        }
    else:
        projection = {
            "emitted": False,
            "scenario_not_forecast": True,
            "basis_day_count": sample_count,
            "sample_uncertainty": uncertainty,
            "uncertainty_is_not_a_confidence_interval": True,
            "reason": (
                "no valid saved_at day"
                if not values
                else "table is a replacement/rolling cache; cumulative arithmetic would be misleading"
            ),
        }
    return {
        "actual_database_growth_observed": False,
        "proxy_definition": (
            "bytes of currently surviving rows assigned to the UTC day of "
            "their current saved_at value"
        ),
        "saved_at_day_count": len(daily),
        "boundary_complete_day_count": len(complete),
        "boundary_complete_definition": (
            "first surviving saved_at at or before 00:15 UTC and last at or "
            "after 23:45 UTC; this does not prove uninterrupted ingestion"
        ),
        "by_saved_at_day": daily,
        "projection": projection,
    }


def _storage_semantics(
    *,
    table: str,
    primary_key: list[str],
    columns: list[str],
    writer_path: Path | None,
) -> dict[str, Any]:
    identity = ["region_id", "source", "nominal_timestamp"]
    upsert = _writer_has_upsert(writer_path, table)
    revision_columns = [
        column
        for column in columns
        if column.casefold() in {"revision", "revision_id", "version", "version_id"}
    ]
    one_row_identity = primary_key == identity
    return {
        "primary_key_columns": primary_key,
        "single_current_row_per_region_provider_nominal_timestamp": one_row_identity,
        "explicit_revision_columns": revision_columns,
        "append_only_revision_history_available": bool(revision_columns),
        "writer_source_checked": str(writer_path) if writer_path else None,
        "on_conflict_do_update_detected_in_writer": upsert,
        "prior_revisions_can_be_overwritten": bool(one_row_identity and upsert),
        "historical_revision_count_reconstructable": False,
        "interpretation": (
            "The primary key permits only one surviving row per identity and "
            "the repository writer updates conflicts; prior payload revisions "
            "and their sizes are therefore not recoverable from this table."
            if one_row_identity and upsert
            else "Schema permits only one current row per identity; writer upsert confirmation was not available."
        ),
    }


def _schema(
    database: sqlite3.Connection,
    table: str,
) -> tuple[list[str], list[str], str]:
    rows = database.execute(f'PRAGMA table_info("{table}")').fetchall()
    columns = [str(row["name"]) for row in rows]
    primary_key = [
        str(row["name"])
        for row in sorted(rows, key=lambda value: int(value["pk"]))
        if int(row["pk"]) > 0
    ]
    sql_row = database.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    table_sql = str(sql_row["sql"] or "") if sql_row else ""
    return columns, primary_key, table_sql


def _schema_report(
    columns: list[str],
    primary_key: list[str],
    table_sql: str,
) -> dict[str, Any]:
    return {
        "columns": columns,
        "primary_key_columns": primary_key,
        "without_rowid": "WITHOUT ROWID" in table_sql.upper(),
    }


def _empty_table(*, semantic_label: str, present: bool) -> dict[str, Any]:
    return {
        "semantic_label": semantic_label,
        "present": present,
        "schema": {
            "columns": [],
            "primary_key_columns": [],
            "without_rowid": False,
        },
        "totals": {
            "row_count": 0,
            "stored_payload_bytes": 0,
            "decoded_payload_bytes": 0,
            "decoded_byte_count_rows": 0,
            "decode_status_counts": {},
            "top_level_json_field_counts": {},
            "nominal_time": _temporal_summary(()),
        },
        "by_provider_region": [],
        "by_provider_region_nominal_day": [],
        "by_provider_region_saved_at_day": [],
        "storage_semantics": {},
        "growth_proxy": {
            "actual_database_growth_observed": False,
            "by_saved_at_day": [],
            "projection": {
                "emitted": False,
                "reason": "table absent or required schema unavailable",
            },
        },
        "limitations": [],
    }


def _base_record(row: sqlite3.Row, stored_bytes: int | None) -> dict[str, Any]:
    return {
        "region_id": str(row["region_id"]),
        "source": str(row["source"]),
        "nominal_timestamp": _finite_timestamp(row["nominal_timestamp"]),
        "saved_at": _finite_timestamp(row["saved_at"]),
        "stored_bytes": stored_bytes,
    }


def _group_records(
    records: Iterable[dict[str, Any]],
    key_function: Any,
) -> list[tuple[Any, list[dict[str, Any]]]]:
    groups: dict[Any, list[dict[str, Any]]] = {}
    for record in records:
        groups.setdefault(key_function(record), []).append(record)
    return [(key, groups[key]) for key in sorted(groups)]


def _limited_zlib_decompress(
    payload: bytes,
    *,
    maximum_bytes: int,
    maximum_ratio: float,
) -> bytes:
    """Decode one zlib member with absolute, ratio and trailing-data bounds."""

    if not payload:
        raise ValueError("empty_compressed_payload")
    ratio_bytes = max(1, math.floor(len(payload) * maximum_ratio))
    effective_limit = min(maximum_bytes, ratio_bytes)
    decompressor = zlib.decompressobj()
    decoded = decompressor.decompress(payload, effective_limit + 1)
    if len(decoded) > effective_limit or decompressor.unconsumed_tail:
        reason = (
            "decompression_ratio_limit_exceeded"
            if ratio_bytes <= maximum_bytes
            else "decoded_payload_too_large"
        )
        raise ValueError(reason)
    decoded += decompressor.flush(effective_limit + 1 - len(decoded))
    if len(decoded) > effective_limit:
        reason = (
            "decompression_ratio_limit_exceeded"
            if ratio_bytes <= maximum_bytes
            else "decoded_payload_too_large"
        )
        raise ValueError(reason)
    if not decompressor.eof:
        raise ValueError("incomplete_zlib_stream")
    if decompressor.unused_data:
        raise ValueError("trailing_zlib_data")
    return decoded


def _resolve_writer_source(value: str | Path | None) -> Path | None:
    if value is not None:
        path = Path(value).expanduser().resolve()
        return path if path.is_file() else None
    candidate = (
        Path(__file__).resolve().parents[1]
        / "custom_components"
        / "storm_tracker_v3"
        / "engine"
        / "calibration_store.py"
    )
    return candidate if candidate.is_file() else None


def _writer_has_upsert(path: Path | None, table: str) -> bool:
    if path is None:
        return False
    try:
        source = path.read_text(encoding="utf-8")
    except OSError:
        return False
    marker = f"INSERT INTO {table}"
    start = source.find(marker)
    if start < 0:
        return False
    next_insert = source.find("INSERT INTO ", start + len(marker))
    section = source[start:next_insert if next_insert >= 0 else None]
    compact = " ".join(section.upper().split())
    return "ON CONFLICT" in compact and "DO UPDATE" in compact


def _modal_interval(values: list[float]) -> float | None:
    if not values:
        return None
    rounded = [round(value, 6) for value in values if value > 0]
    if not rounded:
        return None
    counts = Counter(rounded)
    highest = max(counts.values())
    return min(value for value, count in counts.items() if count == highest)


def _nearest_rank(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return _clean_number(ordered[index])


def _finite_timestamp(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _nonnegative_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def _positive_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} moet een positief geheel getal zijn")
    return value


def _positive_finite(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} moet een positief eindig getal zijn")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} moet een positief eindig getal zijn")
    return result


def _utc_day(value: float | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value, timezone.utc).date().isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _iso(value: float | None) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(value, timezone.utc).isoformat()
    except (OverflowError, OSError, ValueError):
        return None


def _clean_number(value: float | int | None) -> float | int | None:
    if value is None:
        return None
    number = float(value)
    return int(number) if number.is_integer() else round(number, 6)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Inventariseer frameopslag in een kalibratiebackup strikt read-only"
        )
    )
    parser.add_argument("database", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--maximum-compressed-bytes",
        type=int,
        default=DEFAULT_MAX_COMPRESSED_BYTES,
    )
    parser.add_argument(
        "--maximum-decoded-bytes",
        type=int,
        default=DEFAULT_MAX_DECODED_BYTES,
    )
    parser.add_argument(
        "--maximum-decompression-ratio",
        type=float,
        default=DEFAULT_MAX_DECOMPRESSION_RATIO,
    )
    parser.add_argument(
        "--maximum-frame-json-bytes",
        type=int,
        default=DEFAULT_MAX_FRAME_JSON_BYTES,
    )
    arguments = parser.parse_args()
    report = audit_capture_size_inventory(
        arguments.database,
        maximum_compressed_bytes=arguments.maximum_compressed_bytes,
        maximum_decoded_bytes=arguments.maximum_decoded_bytes,
        maximum_decompression_ratio=arguments.maximum_decompression_ratio,
        maximum_frame_json_bytes=arguments.maximum_frame_json_bytes,
    )
    rendered = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    if arguments.output is None:
        print(rendered, end="")
    else:
        output = arguments.output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(rendered, encoding="utf-8")
        print(output)
    if not report["database_unchanged"]:
        raise SystemExit("databasehash veranderde tijdens de read-only audit")


if __name__ == "__main__":
    main()
