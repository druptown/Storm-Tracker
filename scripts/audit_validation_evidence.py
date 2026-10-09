"""Read-only eligibility audit for scientific Storm Tracker validation data."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
from statistics import median
from typing import Any


_TRACK_TRUTH_TABLES = frozenset({
    "tracking_ground_truth",
    "golden_track_annotations",
    "cell_lineage_annotations",
})
_RAW_UPSTREAM_TABLES = frozenset({
    "meteorological_raw_blobs",
    "meteorological_raw_receipts",
    "raw_artifact_blobs",
    "raw_artifact_receipts",
})


def audit_validation_evidence(database_path: str | Path) -> dict[str, Any]:
    """Inventory evidence without mutating or treating engine output as truth."""

    path = Path(database_path).expanduser().resolve(strict=True)
    if not path.is_file():
        raise ValueError(f"databasepad is geen bestand: {path}")
    sha_before = _sha256(path)
    uri = f"{path.as_uri()}?mode=ro"
    try:
        database = sqlite3.connect(uri, uri=True)
    except sqlite3.Error as err:
        raise RuntimeError(f"database kan niet read-only open: {err}") from err
    try:
        database.row_factory = sqlite3.Row
        database.execute("PRAGMA query_only=ON")
        tables = {
            str(row["name"])
            for row in database.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        counts = {
            table: int(database.execute(
                f"SELECT count(*) FROM {_quoted_identifier(table)}"
            ).fetchone()[0])
            for table in sorted(tables)
        }
        forecast = _forecast_evidence(database, tables)
        ground = _ground_table_evidence(database, tables)
        performance = _performance_evidence(database, tables)
    except sqlite3.Error as err:
        raise RuntimeError(f"read-only evidenceaudit faalde: {err}") from err
    finally:
        database.close()

    sha_after = _sha256(path)
    has_tracking_truth = bool(tables & _TRACK_TRUTH_TABLES)
    has_full_raw_store = _RAW_UPSTREAM_TABLES <= tables
    ground_positive = (
        forecast["virtual_probe_outcomes"]["numeric_wet_count"]
        + ground["wet_count"]
    )
    ground_negative = (
        forecast["virtual_probe_outcomes"]["numeric_dry_count"]
        + ground["dry_count"]
    )
    probability_count = forecast["passage_probability_count"]

    eligibility = {
        "exploratory_cell_motion": {
            "eligible": counts.get("pysteps_frame_cache", 0) >= 2,
            "reason": (
                "normalized frames support exploratory replay, not truth"
            ),
        },
        "tracking_identity_metrics": {
            "eligible": has_tracking_truth,
            "reason": (
                "requires independently reviewed detection/identity/lineage "
                "annotations split by weather event"
            ),
        },
        "speed_error_metric": {
            "eligible": has_tracking_truth,
            "reason": (
                "two engine positions measure internal displacement; speed "
                "error needs an independently labelled same-cell identity"
            ),
        },
        "ground_wet_sensitivity": {
            "eligible": ground_positive > 0,
            "support": ground_positive,
            "reason": "positive sensor outcomes exist but are selection-biased",
        },
        "ground_dry_specificity": {
            "eligible": ground_negative > 0,
            "support": ground_negative,
            "reason": "requires capable, fresh dry observations at evaluated sites",
        },
        "passage_brier_score": {
            "eligible": (
                probability_count > 0
                and ground_positive > 0
                and ground_negative > 0
            ),
            "probability_count": probability_count,
            "positive_outcomes": ground_positive,
            "negative_outcomes": ground_negative,
            "reason": (
                "requires stored passage probabilities and independently "
                "observed wet plus dry outcomes"
            ),
        },
        "exact_upstream_replay": {
            "eligible": has_full_raw_store,
            "reason": (
                "original_radar_frames contains provider overlays; exact "
                "upstream bytes require raw blob and receipt tables"
            ),
        },
        "thermal_characterization": {
            "eligible": performance["sample_count"] > 0,
            "support": performance["sample_count"],
            "reason": (
                "supports historical diagnostics; a representative live "
                "soak is still required for release"
            ),
        },
    }
    return {
        "audit_version": "storm-tracker-validation-evidence-audit-v1",
        "database": str(path),
        "connection_mode": "sqlite-uri-mode-ro-query-only",
        "database_sha256_before": sha_before,
        "database_sha256_after": sha_after,
        "database_unchanged": sha_before == sha_after,
        "table_counts": counts,
        "forecast_verification": forecast,
        "ground_observations": ground,
        "system_performance": performance,
        "eligibility": eligibility,
        "scientific_warning": (
            "Provider agreement, engine status, centroid displacement and "
            "verification against another forecast are not independent "
            "meteorological ground truth."
        ),
    }


def _forecast_evidence(
    database: sqlite3.Connection,
    tables: set[str],
) -> dict[str, Any]:
    if "forecast_verification_samples" not in tables:
        return {
            "sample_count": 0,
            "sample_types": {},
            "virtual_probe_outcomes": _empty_probe_outcomes(),
            "passage_probability_count": 0,
            "limitations": ["forecast_verification_samples ontbreekt"],
        }
    columns = _columns(database, "forecast_verification_samples")
    required = {"sample_type", "observed_at", "snapshot_json"}
    if not required <= columns:
        return {
            "sample_count": 0,
            "sample_types": {},
            "virtual_probe_outcomes": _empty_probe_outcomes(),
            "passage_probability_count": 0,
            "limitations": [
                "forecast_verification_samples mist vereiste kolommen"
            ],
        }
    rows = database.execute(
        "SELECT sample_type, observed_at, snapshot_json "
        "FROM forecast_verification_samples ORDER BY observed_at"
    ).fetchall()
    sample_types: Counter[str] = Counter()
    outcomes: Counter[str] = Counter()
    station_ids: Counter[str] = Counter()
    radar_sources: Counter[str] = Counter()
    numeric_wet = 0
    numeric_dry = 0
    numeric_unknown = 0
    invalid_json = 0
    probability_count = 0
    timestamps: list[float] = []
    for row in rows:
        sample_type = str(row["sample_type"])
        sample_types[sample_type] += 1
        timestamp = _optional_finite(row["observed_at"])
        if timestamp is not None:
            timestamps.append(timestamp)
        try:
            snapshot = json.loads(str(row["snapshot_json"]))
        except (TypeError, ValueError):
            invalid_json += 1
            continue
        if not isinstance(snapshot, dict):
            invalid_json += 1
            continue
        probability = _find_passage_probability(snapshot)
        if probability is not None:
            probability_count += 1
        if sample_type != "virtual_probe_outcome":
            continue
        outcomes[str(snapshot.get("outcome") or "UNKNOWN")] += 1
        probe = snapshot.get("probe")
        if not isinstance(probe, dict):
            probe = {}
        station_ids[str(probe.get("station_id") or "UNKNOWN")] += 1
        radar_sources[str(probe.get("radar_source") or "UNKNOWN")] += 1
        numeric_values = [
            value
            for value in (
                _optional_finite(snapshot.get("rain_live_mm_h")),
                _optional_finite(snapshot.get("rain_60min_mm")),
            )
            if value is not None
        ]
        if not numeric_values:
            numeric_unknown += 1
        elif any(value > 0.0 for value in numeric_values):
            numeric_wet += 1
        else:
            numeric_dry += 1
    return {
        "sample_count": len(rows),
        "sample_types": dict(sorted(sample_types.items())),
        "first_observed_at": _iso(min(timestamps)) if timestamps else None,
        "last_observed_at": _iso(max(timestamps)) if timestamps else None,
        "invalid_snapshot_json_count": invalid_json,
        "passage_probability_count": probability_count,
        "virtual_probe_outcomes": {
            "count": sum(outcomes.values()),
            "outcomes": dict(sorted(outcomes.items())),
            "station_ids": dict(sorted(station_ids.items())),
            "radar_sources": dict(sorted(radar_sources.items())),
            "numeric_wet_count": numeric_wet,
            "numeric_dry_count": numeric_dry,
            "numeric_unknown_count": numeric_unknown,
        },
        "limitations": [
            "buienradar is another forecast product, not ground truth",
            "own_status and worker parity are engine outputs, not labels",
            "virtual probes are trigger-selected rather than an unbiased stream",
        ],
    }


def _ground_table_evidence(
    database: sqlite3.Connection,
    tables: set[str],
) -> dict[str, Any]:
    if "ground_observations" not in tables:
        return {
            "count": 0,
            "wet_count": 0,
            "dry_count": 0,
            "unknown_count": 0,
            "providers": {},
            "limitations": ["ground_observations ontbreekt in deze backup"],
        }
    columns = _columns(database, "ground_observations")
    required = {"provider", "precipitation_present"}
    if not required <= columns:
        return {
            "count": 0,
            "wet_count": 0,
            "dry_count": 0,
            "unknown_count": 0,
            "providers": {},
            "limitations": ["ground_observations mist vereiste kolommen"],
        }
    rows = database.execute(
        "SELECT provider, precipitation_present FROM ground_observations"
    ).fetchall()
    providers = Counter(str(row["provider"]) for row in rows)
    wet = sum(row["precipitation_present"] == 1 for row in rows)
    dry = sum(row["precipitation_present"] == 0 for row in rows)
    unknown = len(rows) - wet - dry
    return {
        "count": len(rows),
        "wet_count": wet,
        "dry_count": dry,
        "unknown_count": unknown,
        "providers": dict(sorted(providers.items())),
        "limitations": [
            "capability, freshness, interval and site independence must also "
            "be validated before records become ground truth"
        ],
    }


def _performance_evidence(
    database: sqlite3.Connection,
    tables: set[str],
) -> dict[str, Any]:
    if "system_performance_samples" not in tables:
        return {
            "sample_count": 0,
            "cycle_count": 0,
            "temperature_reading_count": 0,
            "maximum_temperature_c": None,
            "after_at_or_above_90c": 0,
            "after_at_or_above_95c": 0,
            "after_at_or_above_100c": 0,
            "sample_peak_at_or_above_90c": 0,
            "sample_peak_at_or_above_95c": 0,
            "sample_peak_at_or_above_100c": 0,
            "cycle_peak_at_or_above_90c": 0,
            "cycle_peak_at_or_above_95c": 0,
            "cycle_peak_at_or_above_100c": 0,
            "by_stage": {},
        }
    rows = database.execute(
        "SELECT cycle_id, stage, duration_ms, outcome, cpu_after, "
        "temperature_before_c, temperature_after_c "
        "FROM system_performance_samples"
    ).fetchall()
    before_values = [
        value
        for row in rows
        if (value := _optional_finite(row["temperature_before_c"])) is not None
    ]
    after_values = [
        value
        for row in rows
        if (value := _optional_finite(row["temperature_after_c"])) is not None
    ]
    values = [*before_values, *after_values]
    sample_peaks: list[float] = []
    for row in rows:
        readings = [
            value
            for value in (
                _optional_finite(row["temperature_before_c"]),
                _optional_finite(row["temperature_after_c"]),
            )
            if value is not None
        ]
        if readings:
            sample_peaks.append(max(readings))
    cycle_peaks: dict[str, float] = {}
    for row in rows:
        readings = [
            value
            for value in (
                _optional_finite(row["temperature_before_c"]),
                _optional_finite(row["temperature_after_c"]),
            )
            if value is not None
        ]
        if not readings:
            continue
        cycle_id = str(row["cycle_id"])
        cycle_peaks[cycle_id] = max(
            cycle_peaks.get(cycle_id, float("-inf")),
            *readings,
        )
    return {
        "sample_count": len(rows),
        "cycle_count": len({str(row["cycle_id"]) for row in rows}),
        "temperature_reading_count": len(values),
        "maximum_temperature_c": max(values) if values else None,
        "after_at_or_above_90c": sum(
            value >= 90.0 for value in after_values
        ),
        "after_at_or_above_95c": sum(
            value >= 95.0 for value in after_values
        ),
        "after_at_or_above_100c": sum(
            value >= 100.0 for value in after_values
        ),
        "sample_peak_at_or_above_90c": sum(
            value >= 90.0 for value in sample_peaks
        ),
        "sample_peak_at_or_above_95c": sum(
            value >= 95.0 for value in sample_peaks
        ),
        "sample_peak_at_or_above_100c": sum(
            value >= 100.0 for value in sample_peaks
        ),
        "cycle_peak_at_or_above_90c": sum(
            value >= 90.0 for value in cycle_peaks.values()
        ),
        "cycle_peak_at_or_above_95c": sum(
            value >= 95.0 for value in cycle_peaks.values()
        ),
        "cycle_peak_at_or_above_100c": sum(
            value >= 100.0 for value in cycle_peaks.values()
        ),
        "by_stage": _performance_by_stage(rows),
    }


def _performance_by_stage(
    rows: list[sqlite3.Row],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(str(row["stage"]), []).append(row)
    result: dict[str, dict[str, Any]] = {}
    for stage, stage_rows in sorted(grouped.items()):
        durations = [
            value
            for row in stage_rows
            if (value := _optional_finite(row["duration_ms"])) is not None
        ]
        temperatures = [
            value
            for row in stage_rows
            if (
                value := _optional_finite(row["temperature_after_c"])
            ) is not None
        ]
        cpu = [
            value
            for row in stage_rows
            if (value := _optional_finite(row["cpu_after"])) is not None
        ]
        outcomes = Counter(str(row["outcome"]) for row in stage_rows)
        result[stage] = {
            "sample_count": len(stage_rows),
            "total_duration_ms": sum(durations),
            "median_duration_ms": median(durations) if durations else None,
            "p95_duration_ms": _nearest_rank(durations, 0.95),
            "maximum_duration_ms": max(durations) if durations else None,
            "median_cpu_after_percent": median(cpu) if cpu else None,
            "maximum_cpu_after_percent": max(cpu) if cpu else None,
            "median_temperature_after_c": (
                median(temperatures) if temperatures else None
            ),
            "maximum_temperature_after_c": (
                max(temperatures) if temperatures else None
            ),
            "after_at_or_above_100c": sum(
                value >= 100.0 for value in temperatures
            ),
            "outcomes": dict(sorted(outcomes.items())),
        }
    return result


def _nearest_rank(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[index]


def _empty_probe_outcomes() -> dict[str, Any]:
    return {
        "count": 0,
        "outcomes": {},
        "station_ids": {},
        "radar_sources": {},
        "numeric_wet_count": 0,
        "numeric_dry_count": 0,
        "numeric_unknown_count": 0,
    }


def _columns(database: sqlite3.Connection, table: str) -> set[str]:
    return {
        str(row["name"])
        for row in database.execute(
            f"PRAGMA table_info({_quoted_identifier(table)})"
        )
    }


def _quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _find_passage_probability(snapshot: dict[str, Any]) -> float | None:
    for key in ("passage_probability", "precipitation_probability"):
        value = _optional_finite(snapshot.get(key))
        if value is not None and 0.0 <= value <= 1.0:
            return value
    forecast = snapshot.get("forecast")
    if isinstance(forecast, dict):
        return _find_passage_probability(forecast)
    return None


def _optional_finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    numeric = float(value)
    return numeric if numeric == numeric and abs(numeric) != float("inf") else None


def _iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Audit read-only welke wetenschappelijke validatie de "
            "kalibratiedatabase wel en niet ondersteunt."
        )
    )
    parser.add_argument("database")
    parser.add_argument("--output")
    parser.add_argument("--compact", action="store_true")
    args = parser.parse_args()
    report = audit_validation_evidence(args.database)
    rendered = json.dumps(
        report,
        indent=None if args.compact else 2,
        sort_keys=True,
        ensure_ascii=False,
    )
    if args.output:
        output = Path(args.output).expanduser().resolve()
        output.write_text(rendered + "\n", encoding="utf-8")
        print(output)
    else:
        print(rendered)


if __name__ == "__main__":
    main()
