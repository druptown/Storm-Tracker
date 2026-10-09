def _overlay(timestamp=1_234.0, *, source="opera", runs=()):
    return {"source": source, "timestamp": timestamp, "runs": list(runs)}


def _run(intensity=7, west=4.0, south=50.0, east=4.04, north=50.04):
    return {
        "intensity": intensity,
        "ring": ((south, west), (south, east), (north, east), (north, west)),
    }


def test_overlay_becomes_minute_aligned_sparse_regular_raster(radar_frame_module):
    frame = radar_frame_module.frame_from_overlay(
        _overlay(runs=[_run()]), (3.9, 49.9, 4.1, 50.1)
    )
    assert frame.source == "opera"
    assert frame.nominal_timestamp == 1_200
    assert frame.height == 10
    assert frame.width == 10
    assert frame.wet_cells == 9
    assert frame.to_numpy().max() == 7


def test_invalid_overlay_is_rejected_without_a_dry_artifact(radar_frame_module):
    assert radar_frame_module.frame_from_overlay(
        {"source": "opera", "timestamp": "not-a-time", "runs": []},
        (3.9, 49.9, 4.1, 50.1),
    ) is None


def test_history_replaces_the_same_minute_and_remains_bounded(radar_frame_module):
    history = radar_frame_module.RadarFrameHistory(max_frames=3)
    bounds = (3.9, 49.9, 4.1, 50.1)
    history.add_overlay("region-1", _overlay(1_201, runs=[_run(4)]), bounds)
    history.add_overlay("region-1", _overlay(1_239, runs=[_run(9)]), bounds)
    for timestamp in (1_260, 1_320, 1_380):
        history.add_overlay("region-1", _overlay(timestamp, runs=[]), bounds)
    diagnostics = history.diagnostics()
    assert diagnostics["regions"]["region-1"]["frames"] == 3
    assert diagnostics["regions"]["region-1"]["latest_nominal_timestamp"] == 1_380
    assert diagnostics["feeds_detection"] is True


def test_recent_serialized_frames_survive_restart(radar_frame_module):
    bounds = (3.9, 49.9, 4.1, 50.1)
    original = radar_frame_module.RadarFrameHistory(max_frames=3)
    for timestamp in (1_200, 1_800, 2_400):
        original.add_overlay(
            "region-1", _overlay(timestamp, source="rainviewer", runs=[_run()]),
            bounds,
        )
    restored = radar_frame_module.RadarFrameHistory(max_frames=3)
    count = restored.restore(
        original.serialized(),
        {"region-1": bounds},
        now=2_460,
    )

    assert count == 3
    assert restored.frames("region-1") == original.frames("region-1")


def test_restore_rejects_stale_or_changed_region_grid(radar_frame_module):
    bounds = (3.9, 49.9, 4.1, 50.1)
    original = radar_frame_module.RadarFrameHistory(max_frames=3)
    original.add_overlay(
        "region-1", _overlay(1_200, source="rainviewer", runs=[_run()]),
        bounds,
    )
    restored = radar_frame_module.RadarFrameHistory(max_frames=3)

    assert restored.restore(
        original.serialized(),
        {"region-1": (4.0, 50.0, 4.2, 50.2)},
        now=1_260,
    ) == 0
    assert restored.restore(
        original.serialized(),
        {"region-1": bounds},
        now=10_000,
    ) == 0


def test_restart_audit_detects_missing_and_restored_storm_object(
    radar_frame_module,
):
    frame = radar_frame_module.frame_from_overlay(
        _overlay(1_200, source="rainviewer", runs=[_run()]),
        (3.9, 49.9, 4.1, 50.1),
    )

    missing = radar_frame_module.audit_storm_object_recovery((frame,), [])
    assert missing["status"] == "ontbrekend_stormobject"

    class RestoredStorm:
        storm_id = "continued"
        last_radar_timestamp = 1_190

    restored = radar_frame_module.audit_storm_object_recovery(
        (frame,), [RestoredStorm()]
    )
    assert restored == {
        "status": "hersteld",
        "latest_cached_frame": 1_200.0,
        "matched_storm_ids": ["continued"],
    }
