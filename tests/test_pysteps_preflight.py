def _frame(module, timestamp, column):
    return module.RadarFrame(
        source="opera", nominal_timestamp=timestamp,
        bounds=(4.0, 50.0, 4.2, 50.2), grid_deg=0.02,
        height=10, width=10, cells=((4, column, 10),), input_runs=1,
    )


def test_preflight_measures_translation_and_verifies_next_frame(
    radar_frame_module, pysteps_preflight_module,
):
    frames = (
        _frame(radar_frame_module, 1_200, 2),
        _frame(radar_frame_module, 1_260, 3),
        _frame(radar_frame_module, 1_320, 4),
    )
    result = pysteps_preflight_module.evaluate_frames(frames)
    assert result["status"] == "ready"
    assert result["translation_km_per_hour"] > 0
    assert result["heading_deg"] == 90
    assert result["verification_iou"] == 1.0
    assert result["feeds_detection"] is False


def test_preflight_refuses_a_changed_raster(radar_frame_module, pysteps_preflight_module):
    first = _frame(radar_frame_module, 1_200, 2)
    changed = radar_frame_module.RadarFrame(
        source="kmi", nominal_timestamp=1_260,
        bounds=first.bounds, grid_deg=first.grid_deg,
        height=first.height, width=first.width, cells=first.cells, input_runs=1,
    )
    result = pysteps_preflight_module.evaluate_frames((first, changed))
    assert result["status"] == "incompatible_frames"
