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
    with sqlite3.connect(path) as db:
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
    assert stats["schema_version"] == 4
    assert stats["total_frames"] == 0
    assert stats["total_datapoints"] == 0
    assert stats["total_comparisons"] == 0
    assert stats["total_verification_samples"] == 0
    assert stats["total_bias_samples"] == 0
    assert stats["last_reset_at"]
    assert "coverage_contract_clean_reset" in stats["last_reset_reason"]
    assert store.load_bias_profiles() == []
    with sqlite3.connect(path) as db:
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
