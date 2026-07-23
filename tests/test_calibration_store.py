import sqlite3


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
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM frames").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM frame_points").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM comparisons").fetchone()[0] == 1


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
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT count(*) FROM frames").fetchone()[0] == 1
        assert db.execute(
            "SELECT max_intensity FROM frame_points"
        ).fetchone()[0] == 5.0


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
    with sqlite3.connect(store.path) as db:
        row = db.execute(
            "SELECT buienradar_average_mm_h, own_eta_minutes, snapshot_json "
            "FROM forecast_verification_samples WHERE sample_type='cycle'"
        ).fetchone()
    assert row[0:2] == (0.4, 25.0)
    assert '"eta_reliable":true' in row[2]
