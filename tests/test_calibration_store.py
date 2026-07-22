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
    with sqlite3.connect(store.path) as db:
        assert db.execute("SELECT count(*) FROM frames").fetchone()[0] == 1
        assert db.execute(
            "SELECT max_intensity FROM frame_points"
        ).fetchone()[0] == 5.0
