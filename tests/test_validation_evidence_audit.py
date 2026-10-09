"""Tests for calibration validation-evidence eligibility auditing."""
from __future__ import annotations

from contextlib import closing
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "audit_validation_evidence.py"
SPEC = importlib.util.spec_from_file_location("validation_evidence_audit", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _database(path: Path) -> None:
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript(
            """
            CREATE TABLE forecast_verification_samples (
                sample_type TEXT NOT NULL,
                observed_at REAL NOT NULL,
                snapshot_json TEXT NOT NULL
            );
            CREATE TABLE pysteps_frame_cache (
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_timestamp INTEGER NOT NULL,
                saved_at REAL NOT NULL,
                frame_json TEXT NOT NULL
            );
            CREATE TABLE system_performance_samples (
                cycle_id TEXT NOT NULL,
                stage TEXT NOT NULL,
                duration_ms REAL NOT NULL,
                outcome TEXT NOT NULL,
                cpu_after REAL,
                temperature_before_c REAL,
                temperature_after_c REAL
            );
            """
        )
        wet = {
            "outcome": "regen_bevestigd",
            "rain_live_mm_h": 1.2,
            "rain_60min_mm": 0.4,
            "probe": {
                "station_id": "KABC",
                "radar_source": "rainviewer",
            },
        }
        dry = {
            "outcome": "geen_regen",
            "rain_live_mm_h": 0.0,
            "rain_60min_mm": 0.0,
            "probe": {
                "station_id": "KDEF",
                "radar_source": "rainviewer",
            },
        }
        unknown = {
            "outcome": "radarbeeld_verouderd",
            "rain_live_mm_h": None,
            "rain_60min_mm": None,
            "probe": {
                "station_id": "KGHI",
                "radar_source": "rainviewer",
            },
        }
        db.executemany(
            "INSERT INTO forecast_verification_samples VALUES(?,?,?)",
            [
                ("virtual_probe_outcome", 1_800_000_000.0, json.dumps(wet)),
                ("virtual_probe_outcome", 1_800_000_060.0, json.dumps(dry)),
                ("virtual_probe_outcome", 1_800_000_120.0, json.dumps(unknown)),
            ],
        )
        db.executemany(
            "INSERT INTO pysteps_frame_cache VALUES(?,?,?,?,?)",
            [
                ("region-1", "kmi", 1, 1.0, "{}"),
                ("region-1", "kmi", 2, 2.0, "{}"),
            ],
        )
        db.executemany(
            "INSERT INTO system_performance_samples VALUES(?,?,?,?,?,?,?)",
            [
                ("cycle-1", "radar", 100.0, "ok", 10.0, 89.0, 96.0),
                ("cycle-2", "radar", 200.0, "ok", 20.0, 99.0, 100.0),
            ],
        )


def test_audit_is_read_only_and_does_not_promote_engine_output_to_truth(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _database(path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()

    report = MODULE.audit_validation_evidence(path)

    after = hashlib.sha256(path.read_bytes()).hexdigest()
    assert before == after
    assert report["database_unchanged"] is True
    assert report["connection_mode"] == "sqlite-uri-mode-ro-query-only"
    assert report["eligibility"]["exploratory_cell_motion"]["eligible"] is True
    assert (
        report["eligibility"]["tracking_identity_metrics"]["eligible"]
        is False
    )
    assert report["eligibility"]["speed_error_metric"]["eligible"] is False
    assert report["eligibility"]["exact_upstream_replay"]["eligible"] is False


def test_probe_wet_dry_and_unknown_support_stays_separate(tmp_path):
    path = tmp_path / "calibration.sqlite3"
    _database(path)

    report = MODULE.audit_validation_evidence(path)
    outcomes = report["forecast_verification"]["virtual_probe_outcomes"]

    assert outcomes["numeric_wet_count"] == 1
    assert outcomes["numeric_dry_count"] == 1
    assert outcomes["numeric_unknown_count"] == 1
    assert report["eligibility"]["ground_wet_sensitivity"]["support"] == 1
    assert report["eligibility"]["ground_dry_specificity"]["support"] == 1
    assert report["eligibility"]["passage_brier_score"]["eligible"] is False
    assert (
        report["eligibility"]["passage_brier_score"]["probability_count"] == 0
    )


def test_performance_inventory_reports_thermal_evidence_without_soak_claim(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _database(path)

    performance = MODULE.audit_validation_evidence(path)["system_performance"]

    assert performance["sample_count"] == 2
    assert performance["cycle_count"] == 2
    assert performance["temperature_reading_count"] == 4
    assert performance["maximum_temperature_c"] == 100.0
    assert performance["after_at_or_above_90c"] == 2
    assert performance["after_at_or_above_95c"] == 2
    assert performance["after_at_or_above_100c"] == 1
    assert performance["sample_peak_at_or_above_90c"] == 2
    assert performance["sample_peak_at_or_above_95c"] == 2
    assert performance["sample_peak_at_or_above_100c"] == 1
    assert performance["cycle_peak_at_or_above_90c"] == 2
    assert performance["cycle_peak_at_or_above_95c"] == 2
    assert performance["cycle_peak_at_or_above_100c"] == 1
    assert performance["by_stage"]["radar"] == {
        "sample_count": 2,
        "total_duration_ms": 300.0,
        "median_duration_ms": 150.0,
        "p95_duration_ms": 200.0,
        "maximum_duration_ms": 200.0,
        "median_cpu_after_percent": 15.0,
        "maximum_cpu_after_percent": 20.0,
        "median_temperature_after_c": 98.0,
        "maximum_temperature_after_c": 100.0,
        "after_at_or_above_100c": 1,
        "outcomes": {"ok": 2},
    }


def test_ground_truth_tables_change_eligibility_but_not_create_labels(
    tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    _database(path)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute("CREATE TABLE golden_track_annotations(id TEXT)")

    report = MODULE.audit_validation_evidence(path)

    assert report["eligibility"]["tracking_identity_metrics"]["eligible"] is True
    assert report["eligibility"]["speed_error_metric"]["eligible"] is True


def test_unusual_sqlite_table_name_is_quoted_not_executed(tmp_path):
    path = tmp_path / "calibration.sqlite3"
    _database(path)
    with closing(sqlite3.connect(path)) as db, db:
        db.execute('CREATE TABLE "odd""name"(value TEXT)')
        db.execute('INSERT INTO "odd""name" VALUES(?)', ("preserved",))

    report = MODULE.audit_validation_evidence(path)

    assert report["table_counts"]['odd"name'] == 1
