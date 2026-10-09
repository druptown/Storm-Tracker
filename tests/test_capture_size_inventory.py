"""Tests for the read-only calibration capture-size inventory."""
from __future__ import annotations

from contextlib import closing
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import zlib

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "audit_capture_size_inventory.py"
SPEC = importlib.util.spec_from_file_location("capture_size_inventory", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _epoch(value: str) -> float:
    return datetime.fromisoformat(value).replace(tzinfo=timezone.utc).timestamp()


def _writer(path: Path) -> None:
    path.write_text(
        """
db.executemany('''
INSERT INTO original_radar_frames(
    region_id, source, nominal_timestamp, saved_at, encoding, payload
) VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(region_id, source, nominal_timestamp)
DO UPDATE SET saved_at=excluded.saved_at, payload=excluded.payload
''', rows)
db.executemany('''
INSERT INTO pysteps_frame_cache(
    region_id, source, nominal_timestamp, saved_at, frame_json
) VALUES(?, ?, ?, ?, ?)
ON CONFLICT(region_id, source, nominal_timestamp)
DO UPDATE SET saved_at=excluded.saved_at, frame_json=excluded.frame_json
''', rows)
""",
        encoding="utf-8",
    )


def _database(path: Path) -> None:
    with closing(sqlite3.connect(path)) as database, database:
        database.executescript(
            """
            CREATE TABLE original_radar_frames (
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_timestamp INTEGER NOT NULL,
                saved_at REAL NOT NULL,
                encoding TEXT NOT NULL,
                payload BLOB NOT NULL,
                PRIMARY KEY(region_id, source, nominal_timestamp)
            ) WITHOUT ROWID;
            CREATE TABLE pysteps_frame_cache (
                region_id TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_timestamp INTEGER NOT NULL,
                saved_at REAL NOT NULL,
                frame_json TEXT NOT NULL,
                PRIMARY KEY(region_id, source, nominal_timestamp)
            ) WITHOUT ROWID;
            """
        )
        overlays = [
            (
                "r1",
                "provider-a",
                1_800_000_000,
                _epoch("2027-01-15T00:05:00"),
                "zlib-json-v1",
                zlib.compress(json.dumps({
                    "runs": [],
                    "headers": {"etag": "x"},
                    "qc": "ok",
                    "nodata_value": -999,
                }).encode()),
            ),
            (
                "r1",
                "provider-a",
                1_800_000_600,
                _epoch("2027-01-15T12:00:00"),
                "zlib-json-v1",
                zlib.compress(json.dumps({"runs": [1]}).encode()),
            ),
            (
                "r1",
                "provider-a",
                1_800_001_800,
                _epoch("2027-01-15T23:50:00"),
                "future-encoding",
                b"opaque",
            ),
        ]
        database.executemany(
            "INSERT INTO original_radar_frames VALUES(?,?,?,?,?,?)",
            overlays,
        )
        database.executemany(
            "INSERT INTO pysteps_frame_cache VALUES(?,?,?,?,?)",
            [
                (
                    "r1",
                    "provider-a",
                    1_800_000_000,
                    _epoch("2027-01-15T20:00:00"),
                    json.dumps({"cells": [], "height": 1, "width": 1}),
                ),
                (
                    "r1",
                    "provider-a",
                    1_800_000_600,
                    _epoch("2027-01-15T20:10:00"),
                    json.dumps({"cells": [[0, 0, 1]], "height": 1, "width": 1}),
                ),
            ],
        )


def test_inventory_is_read_only_and_reports_honest_storage_semantics(
    tmp_path,
):
    database_path = tmp_path / "calibration.sqlite3"
    writer_path = tmp_path / "writer.py"
    _database(database_path)
    _writer(writer_path)
    before = hashlib.sha256(database_path.read_bytes()).hexdigest()

    report = MODULE.audit_capture_size_inventory(
        database_path,
        writer_source_path=writer_path,
    )

    after = hashlib.sha256(database_path.read_bytes()).hexdigest()
    assert before == after
    assert report["database_unchanged"] is True
    assert report["connection_mode"] == "sqlite-uri-mode-ro-query-only"
    original = report["tables"]["original_radar_frames"]
    assert original["semantic_label"] == "provider_overlay_not_upstream_raw"
    assert original["totals"]["row_count"] == 3
    assert original["totals"]["decoded_byte_count_rows"] == 2
    assert original["totals"]["decode_status_counts"][
        "unsupported_encoding:future-encoding"
    ] == 1
    assert len(original["by_provider_region_nominal_day"]) == 1
    time = original["by_provider_region"][0]["nominal_time"]
    assert time["cadence_seconds"] == 600
    assert time["gap_count"] == 1
    assert time["estimated_missing_frames"] == 1
    metadata = original["totals"]["top_level_metadata_evidence"]
    assert metadata["rows_with_explicit_top_level_field"] == {
        "raw_headers": 1,
        "quality_or_qc": 1,
        "nodata_or_validity": 1,
    }
    semantics = original["storage_semantics"]
    assert semantics["primary_key_columns"] == [
        "region_id",
        "source",
        "nominal_timestamp",
    ]
    assert semantics["on_conflict_do_update_detected_in_writer"] is True
    assert semantics["prior_revisions_can_be_overwritten"] is True
    assert semantics["historical_revision_count_reconstructable"] is False
    for grouping in (
        "by_provider_region",
        "by_provider_region_nominal_day",
        "by_provider_region_saved_at_day",
    ):
        assert sum(item["row_count"] for item in original[grouping]) == 3
        assert (
            sum(item["stored_payload_bytes"] for item in original[grouping])
            == original["totals"]["stored_payload_bytes"]
        )


def test_growth_is_surviving_saved_at_proxy_with_explicit_uncertainty(
    tmp_path,
):
    database_path = tmp_path / "calibration.sqlite3"
    _database(database_path)

    report = MODULE.audit_capture_size_inventory(database_path)
    growth = report["tables"]["original_radar_frames"]["growth_proxy"]

    assert growth["actual_database_growth_observed"] is False
    assert growth["boundary_complete_day_count"] == 1
    scenario = growth["projection"]
    assert scenario["emitted"] is True
    assert scenario["scenario_not_forecast"] is True
    assert scenario["basis_day_count"] == 1
    assert scenario["sample_uncertainty"] == "very_high"
    assert (
        scenario["additional_payload_scenarios"]["30"]["central_bytes"]
        == scenario["central_daily_stored_payload_bytes"] * 30
    )
    cache_growth = report["tables"]["pysteps_frame_cache"]["growth_proxy"]
    assert cache_growth["projection"]["emitted"] is False
    assert "rolling cache" in cache_growth["projection"]["reason"]


def test_decompression_has_absolute_ratio_and_stream_integrity_bounds(
    tmp_path,
):
    database_path = tmp_path / "calibration.sqlite3"
    with closing(sqlite3.connect(database_path)) as database, database:
        database.executescript(
            """
            CREATE TABLE original_radar_frames (
                region_id TEXT, source TEXT, nominal_timestamp INTEGER,
                saved_at REAL, encoding TEXT, payload BLOB,
                PRIMARY KEY(region_id, source, nominal_timestamp)
            ) WITHOUT ROWID;
            CREATE TABLE pysteps_frame_cache (
                region_id TEXT, source TEXT, nominal_timestamp INTEGER,
                saved_at REAL, frame_json TEXT,
                PRIMARY KEY(region_id, source, nominal_timestamp)
            ) WITHOUT ROWID;
            """
        )
        bomb = zlib.compress(json.dumps({"value": "x" * 20_000}).encode())
        trailing = zlib.compress(b'{"ok":true}') + b"trailing"
        database.executemany(
            "INSERT INTO original_radar_frames VALUES(?,?,?,?,?,?)",
            [
                ("r", "ratio", 1, 1.0, "zlib-json-v1", bomb),
                ("r", "trailing", 2, 2.0, "zlib-json-v1", trailing),
                ("r", "too-big", 3, 3.0, "zlib-json-v1", b"x" * 512),
            ],
        )

    report = MODULE.audit_capture_size_inventory(
        database_path,
        maximum_compressed_bytes=128,
        maximum_decoded_bytes=1_000_000,
        maximum_decompression_ratio=5,
    )
    statuses = report["tables"]["original_radar_frames"]["totals"][
        "decode_status_counts"
    ]

    assert statuses["decompression_ratio_limit_exceeded"] == 1
    assert statuses["trailing_zlib_data"] == 1
    assert statuses["compressed_payload_too_large"] == 1


def test_absent_tables_and_invalid_limits_fail_closed(tmp_path):
    database_path = tmp_path / "empty.sqlite3"
    with closing(sqlite3.connect(database_path)):
        pass

    report = MODULE.audit_capture_size_inventory(database_path)

    assert report["tables"]["original_radar_frames"]["present"] is False
    assert report["tables"]["pysteps_frame_cache"]["present"] is False
    assert report["combined_current_snapshot"]["surviving_row_count"] == 0
    with pytest.raises(ValueError, match="positief"):
        MODULE.audit_capture_size_inventory(
            database_path,
            maximum_decompression_ratio=0,
        )
