from contextlib import closing
import sqlite3
import time

import pytest


def test_store_persists_frames_points_and_comparisons(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    result = store.write_batch({
        "frames": ((
            "region-1@51.00,4.00,350", "kmi", 100, 6000.0, 6010.0,
            0.1, 2, 1, ((510, 40, 4.0, 0.9, 2),),
        ),),
        "comparisons": ((
            "region-1@51.00,4.00,350", "kmi", "opera", 100, 6000.0,
            2, 3, 2, 0, 1, 1.0, 0.667, 0.8,
        ),),
    })
    assert result["frames_written"] == 1
    assert result["comparisons_written"] == 1
    assert result["total_frames"] == 1
    assert result["total_datapoints"] == 1
    assert result["total_comparisons"] == 1
    assert result["sources"] == 1
    assert result["regions"] == 1
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT count(*) FROM frames").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM frame_points").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM comparisons").fetchone()[0] == 1
    repeated = store.write_batch({
        "frames": ((
            "region-1@51.00,4.00,350", "kmi", 100, 6000.0, 9999.0,
            0.1, 2, 1, ((510, 40, 4.0, 0.9, 2),),
        ),),
        "comparisons": ((
            "region-1@51.00,4.00,350", "kmi", "opera", 100, 9999.0,
            2, 3, 2, 0, 1, 1.0, 0.667, 0.8,
        ),),
    })
    assert repeated["frames_written"] == 0
    assert repeated["comparisons_written"] == 0


def test_original_radar_frames_round_trip_without_regridding(
    calibration_store_module, tmp_path
):
    path = tmp_path / "original.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    overlay = {
        "source": "kmi",
        "timestamp": 1_700_000_000.0,
        "native_shape": [490, 640],
        "runs": [{
            "intensity": 4,
            "ring": [
                [51.01, 4.01], [51.01, 4.03],
                [50.99, 4.03], [50.99, 4.01],
            ],
        }],
    }

    assert store.save_original_radar_frames([{
        "region_id": "region-1",
        "source": "kmi",
        "nominal_timestamp": 1_700_000_000,
        "overlay": overlay,
    }]) == 1

    restored = store.load_original_radar_frames(
        region_id="region-1", source="kmi"
    )
    assert len(restored) == 1
    assert restored[0]["overlay"] == overlay


def test_schema_v11_clears_legacy_calibration_corpus_once(
    calibration_store_module, tmp_path
):
    path = tmp_path / "legacy.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    with closing(sqlite3.connect(path)) as database, database:
        database.execute(
            "UPDATE metadata SET value='10' WHERE key='schema_version'"
        )
        database.execute("""
            INSERT INTO original_radar_frames(
                region_id, source, nominal_timestamp, saved_at,
                encoding, payload
            ) VALUES('old', 'kmi', 1, 1, 'zlib-json-v1', X'00')
        """)

    restarted = calibration_store_module.CalibrationDataStore(str(path))
    statistics = restarted.initialize()

    assert statistics["last_reset_reason"].startswith(
        "schema_v11_original_resolution_clean_reset"
    )
    with closing(sqlite3.connect(path)) as database, database:
        assert database.execute(
            "SELECT COUNT(*) FROM original_radar_frames"
        ).fetchone()[0] == 0


def test_store_persists_system_performance_samples(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    stats = store.initialize()
    assert stats["schema_version"] == 12
    sample = {
        "sample_key": "cycle-1:radar",
        "cycle_id": "cycle-1",
        "stage": "radar",
        "started_at": 100.0,
        "finished_at": 101.25,
        "duration_ms": 1250.0,
        "outcome": "ok",
        "cpu_before": 5.0,
        "cpu_after": 39.0,
        "temperature_before_c": 62.0,
        "temperature_after_c": 100.0,
        "memory_before_percent": 31.0,
        "memory_after_percent": 32.0,
        "load_before": 0.4,
        "load_after": 1.2,
        "fan_before": 0.0,
        "fan_after": 0.0,
        "snapshot": {"before": {"cpu_percent": 5.0}},
    }
    result = store.write_batch({"system_performance_samples": (sample,)})
    assert result["system_performance_samples_written"] == 1
    assert result["total_system_performance_samples"] == 1
    with closing(sqlite3.connect(path)) as db, db:
        row = db.execute(
            "SELECT stage, duration_ms, cpu_after, temperature_after_c, "
            "snapshot_json FROM system_performance_samples"
        ).fetchone()
    assert row[:4] == ("radar", 1250.0, 39.0, 100.0)
    assert '"cpu_percent":5.0' in row[4]


def test_store_merges_compact_lightning_aggregates(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    stats = store.initialize()
    assert stats["schema_version"] == 12
    base = {
        "nominal_minute": 100,
        "source": "blitzortung",
        "grid_lat": 1020,
        "grid_lon": 80,
        "grid_deg": 0.05,
        "strike_count": 2,
        "latitude_sum": 102.01,
        "longitude_sum": 8.01,
        "latitude_min": 51.0,
        "latitude_max": 51.01,
        "longitude_min": 4.0,
        "longitude_max": 4.01,
        "first_timestamp": 6001.0,
        "last_timestamp": 6010.0,
        "region_ids": ["region-1"],
    }
    store.write_batch({"lightning_aggregates": (base,)})
    result = store.write_batch({"lightning_aggregates": ({
        **base,
        "strike_count": 1,
        "latitude_sum": 51.02,
        "longitude_sum": 4.02,
        "latitude_min": 51.02,
        "latitude_max": 51.02,
        "longitude_min": 4.02,
        "longitude_max": 4.02,
        "first_timestamp": 6020.0,
        "last_timestamp": 6020.0,
        "region_ids": ["region-2"],
    },)})
    assert result["total_lightning_aggregate_cells"] == 1
    assert result["total_lightning_strikes"] == 3
    with closing(sqlite3.connect(path)) as db, db:
        row = db.execute(
            "SELECT strike_count, latitude_sum, first_timestamp, "
            "last_timestamp, region_ids_json FROM lightning_aggregates"
        ).fetchone()
    assert row[:4] == (3, 153.03, 6001.0, 6020.0)
    assert row[4] == '["region-1","region-2"]'


def test_store_persists_provider_cycle_samples(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    sample = {
        "sample_key": "netatmo:region-1:100",
        "provider": "netatmo",
        "scope_key": "region-1",
        "nominal_minute": 100,
        "observed_at": 6005.0,
        "latitude": 51.0,
        "longitude": 4.0,
        "metrics": {
            "stations": 52,
            "raining_stations": 2,
            "station_ids": ["a", "b"],
        },
    }
    first = store.write_batch({"provider_cycle_samples": (sample,)})
    second = store.write_batch({"provider_cycle_samples": ({
        **sample,
        "observed_at": 6010.0,
        "metrics": {**sample["metrics"], "raining_stations": 3},
    },)})
    assert first["total_provider_cycle_samples"] == 1
    assert second["total_provider_cycle_samples"] == 1
    with closing(sqlite3.connect(path)) as db, db:
        row = db.execute(
            "SELECT provider, scope_key, observed_at, metrics_json "
            "FROM provider_cycle_samples"
        ).fetchone()
    assert row[:3] == ("netatmo", "region-1", 6010.0)
    assert '"raining_stations":3' in row[3]


def test_failed_batch_restores_in_memory_statistics_after_sqlite_rollback(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "rollback.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    valid = {
        "sample_key": "netatmo:region-1:100",
        "provider": "netatmo",
        "scope_key": "region-1",
        "nominal_minute": 100,
        "observed_at": 6005.0,
        "metrics": {"stations": 1},
    }
    invalid = {**valid, "sample_key": "invalid"}
    invalid.pop("provider")

    with pytest.raises(KeyError):
        store.write_batch({"provider_cycle_samples": (valid, invalid)})

    assert store.statistics()["total_provider_cycle_samples"] == 0
    with closing(sqlite3.connect(path)) as database:
        assert database.execute(
            "SELECT count(*) FROM provider_cycle_samples"
        ).fetchone()[0] == 0


def test_repeated_frame_is_replaced_not_duplicated(
    calibration_store_module, tmp_path,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()
    base = (
        "region", "rainviewer", 100, 6000.0, 6010.0, 0.1, 1, 1,
        ((510, 40, 2.0, None, 1),),
    )
    store.write_batch({"frames": (base,), "comparisons": ()})
    updated = (*base[:6], 2, 1, ((510, 40, 5.0, 0.8, 2),))
    store.write_batch({"frames": (updated,), "comparisons": ()})
    stats = store.statistics()
    assert stats["total_frames"] == 1
    assert stats["total_datapoints"] == 1
    with closing(sqlite3.connect(store.path)) as db, db:
        assert db.execute("SELECT count(*) FROM frames").fetchone()[0] == 1
        assert db.execute(
            "SELECT max_intensity FROM frame_points"
        ).fetchone()[0] == 5.0


def test_store_persists_pysteps_frame_cache_across_instances(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    frame = {
        "source": "rainviewer",
        "nominal_timestamp": int(time.time() // 60) * 60,
        "bounds": [3.9, 49.9, 4.1, 50.1],
        "grid_deg": 0.02,
        "height": 10,
        "width": 10,
        "cells": [[4, 4, 7]],
        "input_runs": 1,
    }
    assert store.save_pysteps_frames({"region-1": [frame]}) == 1

    reopened = calibration_store_module.CalibrationDataStore(str(path))
    reopened.initialize()
    assert reopened.load_pysteps_frames() == {"region-1": [frame]}


def test_store_persists_forecast_and_warning_verification_samples(
    calibration_store_module, tmp_path,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()
    base = {
        "target_id": "home",
        "nominal_minute": 123,
        "observed_at": 7380.0,
        "latitude": 51.0,
        "longitude": 4.0,
        "buienradar_average_mm_h": 0.4,
        "buienradar_total_mm": 1.2,
        "own_status": "naderend",
        "own_distance_km": 20.0,
        "own_eta_minutes": 25.0,
        "own_passage": "rand",
        "own_confidence": "Matig",
        "own_forecast_available": True,
        "warning_stage": "dichtbij",
        "snapshot": {"radar_source": "kmi", "eta_reliable": True},
    }
    result = store.write_batch({
        "frames": (), "comparisons": (),
        "verification_samples": (
            {**base, "sample_key": "cycle:home:123", "sample_type": "cycle"},
            {
                **base,
                "sample_key": "warning:home:123:dichtbij",
                "sample_type": "warning_sent",
            },
        ),
    })
    assert result["total_verification_samples"] == 2
    assert result["total_warning_samples"] == 1
    with closing(sqlite3.connect(store.path)) as db, db:
        row = db.execute(
            "SELECT buienradar_average_mm_h, own_eta_minutes, snapshot_json "
            "FROM forecast_verification_samples WHERE sample_type='cycle'"
        ).fetchone()
    assert row[0:2] == (0.4, 25.0)
    assert '"eta_reliable":true' in row[2]


def test_pending_virtual_probe_restore_includes_expired_recent_forecast(
    calibration_store_module, tmp_path,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()
    probe = {
        "probe_id": "probe-recent",
        "storm_id": "storm-a",
        "region_id": "region-1",
        "station_id": "station-a",
        "latitude": 51.0,
        "longitude": 4.0,
        "created_at": 1_000.0,
        "expected_at": 1_300.0,
        "eta_minutes": 5.0,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Laag",
        "radar_source": "rainviewer",
    }
    store.write_batch({"verification_samples": ({
        "sample_key": "virtual_probe_forecast:probe-recent",
        "sample_type": "virtual_probe_forecast",
        "target_id": "netatmo:station-a",
        "nominal_minute": 16,
        "observed_at": 1_000.0,
        "latitude": 51.0,
        "longitude": 4.0,
        "own_status": "voorspeld",
        "own_forecast_available": True,
        "snapshot": {"schema_version": 1, "probe": probe},
    },)})

    restored = store.load_pending_virtual_probes(now=1_300.0 + 4 * 60 * 60)

    assert restored == [probe]


def test_virtual_probe_history_restores_counters_and_recent_outcomes(
    calibration_store_module, tmp_path,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()

    def outcome(probe_id, value, minute):
        return {
            "sample_key": f"virtual_probe_outcome:{probe_id}",
            "sample_type": "virtual_probe_outcome",
            "target_id": f"netatmo:{probe_id}",
            "nominal_minute": minute,
            "observed_at": minute * 60.0,
            "latitude": 51.0,
            "longitude": 4.0,
            "own_status": value,
            "own_forecast_available": True,
            "snapshot": {
                "schema_version": 1,
                "probe": {
                    "probe_id": probe_id,
                    "storm_id": "storm-a",
                    "station_id": probe_id,
                    "latitude": 51.0,
                    "longitude": 4.0,
                },
                "outcome": value,
                "verified_at": minute * 60.0,
            },
        }

    store.write_batch({"verification_samples": (
        outcome("wet", "regen_bevestigd", 10),
        outcome("dry", "droog_op_verwacht_moment", 11),
        outcome("moved", "traject_gewijzigd", 12),
        outcome("unknown", "niet_verifieerbaar", 13),
    )})

    history = store.load_virtual_probe_history(limit=2)

    assert history["completed"] == 4
    assert history["hits"] == 1
    assert history["misses"] == 1
    assert history["cancelled"] == 1
    assert history["unverifiable"] == 1
    assert [item["probe_id"] for item in history["recent_outcomes"]] == [
        "unknown", "moved",
    ]


def test_store_persists_and_upserts_pysteps_preflight_samples(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    sample = {
        "region_key": "region-1@51.00,4.00,350",
        "source": "rainviewer",
        "nominal_minute": 123,
        "evaluated_at": 7380.0,
        "status": "ready",
        "frames_used": 3,
        "translation_km_per_hour": 42.5,
        "heading_deg": 225.0,
        "verification_iou": 0.61,
        "method": "bounded_grid_match_v1",
        "confidence": "medium",
        "motion_match_iou": 0.7,
        "vector_consistency_deg": 12.0,
        "wet_cell_ratio": 1.1,
        "centroid_translation_km_per_hour": 81.0,
        "centroid_heading_deg": 220.0,
        "reason": None,
    }
    first = store.write_batch({"pysteps_preflight_samples": (sample,)})
    updated = {
        **sample,
        "evaluated_at": 7390.0,
        "verification_iou": 0.72,
    }
    second = store.write_batch({"pysteps_preflight_samples": (updated,)})

    assert first["pysteps_preflight_samples_written"] == 1
    assert second["total_pysteps_preflight_samples"] == 1
    with closing(sqlite3.connect(path)) as db, db:
        row = db.execute(
            "SELECT status, frames_used, translation_km_per_hour, "
            "heading_deg, verification_iou, method, confidence, "
            "motion_match_iou, vector_consistency_deg, wet_cell_ratio, "
            "centroid_translation_km_per_hour, centroid_heading_deg "
            "FROM pysteps_preflight_samples"
        ).fetchone()
    assert row == (
        "ready", 3, 42.5, 225.0, 0.72, "bounded_grid_match_v1", "medium",
        0.7, 12.0, 1.1, 81.0, 220.0,
    )


def test_schema_v4_upgrade_adds_preflight_table_without_losing_data(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    store.write_batch({
        "frames": ((
            "region-1@51.00,4.00,350", "kmi", 100, 6000.0, 6010.0,
            0.1, 1, 1, ((510, 40, 4.0, 0.9, 1),),
        ),),
    })
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE metadata SET value='4' WHERE key='schema_version'"
        )
        db.execute("DROP TABLE pysteps_preflight_samples")

    upgraded = calibration_store_module.CalibrationDataStore(str(path))
    stats = upgraded.initialize()

    assert stats["schema_version"] == 12
    assert stats["total_frames"] == 0
    assert stats["last_reset_at"] is not None
    assert stats["last_reset_reason"].startswith(
        "schema_v11_original_resolution_clean_reset"
    )
    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute("SELECT count(*) FROM frames").fetchone()[0] == 0
        assert db.execute(
            "SELECT count(*) FROM pysteps_preflight_samples"
        ).fetchone()[0] == 0
        columns = {
            row[1] for row in db.execute(
                "PRAGMA table_info(pysteps_preflight_samples)"
            )
        }
    assert "motion_match_iou" in columns
    assert "centroid_translation_km_per_hour" in columns


def test_schema_v5_preflight_rows_survive_v6_column_upgrade(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
            CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            INSERT INTO metadata(key, value) VALUES('schema_version', '5');
            CREATE TABLE pysteps_preflight_samples (
                id INTEGER PRIMARY KEY,
                region_key TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                evaluated_at REAL NOT NULL,
                status TEXT NOT NULL,
                frames_used INTEGER NOT NULL,
                translation_km_per_hour REAL,
                heading_deg REAL,
                verification_iou REAL,
                reason TEXT,
                UNIQUE(region_key, source, nominal_minute)
            );
            INSERT INTO pysteps_preflight_samples(
                region_key, source, nominal_minute, evaluated_at, status,
                frames_used, translation_km_per_hour, heading_deg,
                verification_iou, reason
            ) VALUES(
                'region-2@25.76,-80.19,250', 'rainviewer', 100, 6000,
                'ready', 3, 144.7, 136, 0.065, NULL
            );
        """)

    store = calibration_store_module.CalibrationDataStore(str(path))
    stats = store.initialize()

    assert stats["schema_version"] == 12
    assert stats["total_pysteps_preflight_samples"] == 0
    with closing(sqlite3.connect(path)) as db, db:
        row = db.execute(
            "SELECT translation_km_per_hour, verification_iou, method "
            "FROM pysteps_preflight_samples"
        ).fetchone()
    assert row is None


def test_target_analysis_counts_refresh_on_every_new_sample(
    calibration_store_module, tmp_path,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()

    def sample(target_id, minute):
        return {
            "sample_key": f"cycle:{target_id}:{minute}",
            "sample_type": "target_cycle",
            "target_id": target_id,
            "nominal_minute": minute,
            "observed_at": minute * 60.0,
            "latitude": 51.0,
            "longitude": 4.0,
            "own_forecast_available": False,
            "snapshot": {},
        }

    first_targets = (
        "home",
        "life360_elke_lavrysen",
        "life360_jochem_lavrysen",
        "life360_manuel_van_san",
        "life360_marjolein_van_san",
        "life360_nathan_lavrysen",
        "life360_wim_van_san",
    )
    first = store.write_batch({
        "verification_samples": tuple(
            sample(target_id, 100) for target_id in first_targets
        ),
    })
    assert first["analysis"]["target_samples"] == {
        target_id: 1 for target_id in first_targets
    }

    all_targets = (*first_targets, "test_tracker")
    second_samples = tuple(
        sample(target_id, 101) for target_id in all_targets
    )
    second = store.write_batch({
        "verification_samples": second_samples,
    })
    assert second["analysis"]["sample_types"]["target_cycle"] == 15
    assert len(second["analysis"]["target_samples"]) == 8
    assert second["analysis"]["target_samples"]["test_tracker"] == 1
    for target_id in first_targets:
        assert second["analysis"]["target_samples"][target_id] == 2

    repeated = store.write_batch({
        "verification_samples": second_samples,
    })
    assert repeated["analysis"]["sample_types"]["target_cycle"] == 15
    assert repeated["analysis"]["target_samples"]["test_tracker"] == 1


def test_telemetry_only_flush_never_rescans_legacy_radar_tables(
    calibration_store_module, tmp_path, monkeypatch,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()

    def forbidden(*_args, **_kwargs):
        raise AssertionError("legacy radar scan reached telemetry hot path")

    monkeypatch.setattr(store, "_load_analysis_summary", forbidden)
    monkeypatch.setattr(store, "_load_bias_profiles", forbidden)
    for index in range(15):
        result = store.write_batch({
            "provider_cycle_samples": ({
                "sample_key": f"provider:kmi:{index}",
                "provider": "kmi",
                "scope_key": "51.0,4.0,250",
                "nominal_minute": index,
                "observed_at": float(index * 60),
                "latitude": 51.0,
                "longitude": 4.0,
                "metrics": {"state": "success"},
            },),
        })
        assert result["_bias_profiles"] == []


def test_store_builds_persistent_directional_provider_bias_profiles(
    calibration_store_module, tmp_path,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()
    region = "region-2@45.47,9.19,350"
    frames = (
        (
            region, "dpc_radar", 100, 6000.0, 6010.0, 0.1, 2, 2,
            (
                (454, 91, 10.0, 0.9, 1),
                (454, 92, 20.0, 0.9, 1),
            ),
        ),
        (
            region, "opera", 100, 6030.0, 6040.0, 0.1, 3, 3,
            (
                (454, 91, 12.0, 0.8, 1),
                (454, 92, 22.0, 0.8, 1),
                (455, 92, 5.0, 0.8, 1),
            ),
        ),
    )
    comparison = (
        region, "dpc_radar", "opera", 100, 6040.0,
        2, 3, 2, 0, 1, 1.0, 0.667, 0.8,
    )
    result = store.write_batch({
        "frames": frames,
        "comparisons": (comparison,),
    })
    assert result["total_bias_samples"] == 2
    assert result["total_bias_profiles"] == 4
    profiles = store.load_bias_profiles()
    exact = next(
        profile for profile in profiles
        if profile["scope_key"] == "45.47,9.19,350"
        and profile["from_source"] == "dpc_radar"
        and profile["to_source"] == "opera"
    )
    reverse = next(
        profile for profile in profiles
        if profile["scope_key"] == "45.47,9.19,350"
        and profile["from_source"] == "opera"
        and profile["to_source"] == "dpc_radar"
    )
    assert exact["mean_detection_ratio"] == 1.0
    assert exact["mean_extra_fraction"] == 0.333333
    assert exact["mean_wet_area_ratio"] == 1.5
    assert exact["mean_intensity_bias"] == 2.0
    assert exact["mean_latency_seconds"] == 30.0
    assert reverse["mean_detection_ratio"] == 0.666667
    assert reverse["mean_wet_area_ratio"] == 0.666667
    assert reverse["mean_intensity_bias"] == -2.0
    assert reverse["mean_latency_seconds"] == -30.0


def test_bias_samples_are_upserted_not_double_counted(
    calibration_store_module, tmp_path,
):
    store = calibration_store_module.CalibrationDataStore(
        str(tmp_path / "calibration.sqlite3")
    )
    store.initialize()
    region = "region-1@51.05,4.42,350"
    frames = (
        (
            region, "kmi", 100, 6000.0, 6010.0, 0.1, 1, 1,
            ((510, 44, 4.0, 0.9, 1),),
        ),
        (
            region, "opera", 100, 6000.0, 6010.0, 0.1, 1, 1,
            ((510, 44, 5.0, 0.8, 1),),
        ),
    )
    comparison = (
        region, "kmi", "opera", 100, 6010.0,
        1, 1, 1, 0, 0, 1.0, 1.0, 1.0,
    )
    batch = {"frames": frames, "comparisons": (comparison,)}
    store.write_batch(batch)
    store.write_batch(batch)
    assert store.statistics()["total_bias_samples"] == 2
    assert all(
        profile["sample_count"] == 1
        for profile in store.load_bias_profiles()
    )


def test_pre_v4_history_is_cleanly_reset_for_coverage_contract(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    with closing(sqlite3.connect(path)) as db, db:
        db.executescript("""
            CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE frames(
                id INTEGER PRIMARY KEY,
                region_key TEXT NOT NULL,
                source TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                product_timestamp REAL NOT NULL,
                collected_at REAL NOT NULL,
                grid_deg REAL NOT NULL,
                observation_count INTEGER NOT NULL,
                grid_point_count INTEGER NOT NULL,
                UNIQUE(region_key, source, nominal_minute)
            );
            CREATE TABLE frame_points(
                frame_id INTEGER NOT NULL,
                grid_lat INTEGER NOT NULL,
                grid_lon INTEGER NOT NULL,
                max_intensity REAL,
                max_quality REAL,
                observation_count INTEGER NOT NULL,
                PRIMARY KEY(frame_id, grid_lat, grid_lon)
            ) WITHOUT ROWID;
            CREATE TABLE comparisons(
                id INTEGER PRIMARY KEY,
                region_key TEXT NOT NULL,
                source_a TEXT NOT NULL,
                source_b TEXT NOT NULL,
                nominal_minute INTEGER NOT NULL,
                compared_at REAL NOT NULL,
                primary_cells INTEGER NOT NULL,
                reference_cells INTEGER NOT NULL,
                overlap_cells INTEGER NOT NULL,
                false_positive_cells INTEGER NOT NULL,
                missed_cells INTEGER NOT NULL,
                precision REAL,
                recall REAL,
                f1_score REAL,
                UNIQUE(region_key, source_a, source_b, nominal_minute)
            );
        """)
        region = "region-old@51.05,4.42,350"
        db.executemany("""
            INSERT INTO frames(
                region_key, source, nominal_minute, product_timestamp,
                collected_at, grid_deg, observation_count, grid_point_count
            ) VALUES(?, ?, 100, ?, 6010, 0.1, 1, 1)
        """, (
            (region, "kmi", 6000.0),
            (region, "opera", 6030.0),
        ))
        db.execute("""
            INSERT INTO comparisons(
                region_key, source_a, source_b, nominal_minute, compared_at,
                primary_cells, reference_cells, overlap_cells,
                false_positive_cells, missed_cells, precision, recall, f1_score
            ) VALUES(?, 'kmi', 'opera', 100, 6040, 2, 3, 2, 0, 1, 1, 0.667, 0.8)
        """, (region,))
    store = calibration_store_module.CalibrationDataStore(str(path))
    stats = store.initialize()
    assert stats["schema_version"] == 12
    assert stats["total_frames"] == 0
    assert stats["total_datapoints"] == 0
    assert stats["total_comparisons"] == 0
    assert stats["total_verification_samples"] == 0
    assert stats["total_bias_samples"] == 0
    assert stats["last_reset_at"]
    assert "coverage_contract_clean_reset" in stats["last_reset_reason"]
    assert store.load_bias_profiles() == []
    with closing(sqlite3.connect(path)) as db, db:
        frame_columns = {
            row[1] for row in db.execute("PRAGMA table_info(frames)")
        }
        comparison_columns = {
            row[1] for row in db.execute(
                "PRAGMA table_info(comparisons)"
            )
        }
    assert "coverage_fraction" in frame_columns
    assert "shared_coverage_fraction" in comparison_columns


def test_calibration_reset_never_erases_unknown_operational_tables(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    with closing(sqlite3.connect(path)) as db, db:
        db.execute(
            "UPDATE metadata SET value='10' WHERE key='schema_version'"
        )
        db.execute(
            "CREATE TABLE durable_receipt_sentinel("
            "batch_id TEXT PRIMARY KEY, state TEXT NOT NULL)"
        )
        db.execute(
            "INSERT INTO durable_receipt_sentinel VALUES('batch-1','PREPARED')"
        )

    store.initialize()

    with closing(sqlite3.connect(path)) as db, db:
        assert db.execute(
            "SELECT batch_id, state FROM durable_receipt_sentinel"
        ).fetchall() == [("batch-1", "PREPARED")]


def test_store_persists_ground_provenance_and_decisions(
    calibration_store_module, tmp_path,
):
    path = tmp_path / "calibration.sqlite3"
    store = calibration_store_module.CalibrationDataStore(str(path))
    store.initialize()
    result = store.write_batch({
        "ground_observations": [{
            "record_key": "metar:region-1:KNYC:100",
            "provider": "metar",
            "region_key": "region-1",
            "station_id": "KNYC",
            "observed_at": 6000.0,
            "received_at": 6010.0,
            "latitude": 40.7789,
            "longitude": -73.9692,
            "precipitation_present": True,
            "quantitative_precipitation": False,
            "rain_mm": None,
            "measurement_kind": "categorical_weather_code",
            "parser_version": "metar-v2",
            "raw_text": "METAR KNYC -RA",
            "quality_flags": [],
        }],
        "ground_quality_assessments": [{
            "decision_key": "new-york:region-1:100",
            "observation_key": "metar:region-1:KNYC:100",
            "target_id": "new-york",
            "region_key": "region-1",
            "evaluated_at": 6010.0,
            "station_id": "KNYC",
            "provider": "metar",
            "storm_id": "storm-a",
            "age_seconds": 10.0,
            "distance_target_km": 2.0,
            "distance_storm_km": 0.0,
            "wet": True,
            "fresh": True,
            "eligible": True,
            "quality_score": 0.8,
            "reasons": [],
        }],
        "ground_confirmation_decisions": [{
            "decision_key": "new-york:region-1:100",
            "target_id": "new-york",
            "region_key": "region-1",
            "evaluated_at": 6010.0,
            "confirmed": False,
            "status": "regen_aanwijzing",
            "confidence_percent": 32,
            "score": 0.8,
            "storm_ids": ["storm-a"],
            "sources": ["metar"],
            "snapshot": {"ground_station_count": 1},
        }],
    })

    assert result["total_ground_observations"] == 1
    assert result["total_ground_quality_assessments"] == 1
    assert result["total_ground_confirmation_decisions"] == 1
