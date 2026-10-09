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
    assert result["motion_match_iou"] == 1.0
    assert result["method"] == "bounded_grid_match_v1"
    assert result["search_policy"] == (
        "centroid_guided_fixed_cell_window_v2"
    )
    assert result["confidence"] == "high"
    assert result["feeds_detection"] is True
    assert result["translation_censored"] is False


def test_preflight_refuses_a_changed_raster(radar_frame_module, pysteps_preflight_module):
    first = _frame(radar_frame_module, 1_200, 2)
    changed = radar_frame_module.RadarFrame(
        source="kmi", nominal_timestamp=1_260,
        bounds=first.bounds, grid_deg=first.grid_deg,
        height=first.height, width=first.width, cells=first.cells, input_runs=1,
    )
    result = pysteps_preflight_module.evaluate_frames((first, changed))
    assert result["status"] == "incompatible_frames"


def test_preflight_rejects_inconsistent_vectors(
    radar_frame_module, pysteps_preflight_module,
):
    frames = (
        _frame(radar_frame_module, 1_200, 2),
        _frame(radar_frame_module, 1_260, 3),
        _frame(radar_frame_module, 1_320, 2),
    )
    result = pysteps_preflight_module.evaluate_frames(frames)
    assert result["status"] == "unstable_motion"
    assert result["confidence"] == "low"
    assert result["vector_consistency_deg"] == 180.0
    assert result["feeds_detection"] is False


def test_grid_match_is_not_dragged_by_intensity_centroid(
    radar_frame_module, pysteps_preflight_module,
):
    def frame(timestamp, right_intensity):
        return radar_frame_module.RadarFrame(
            source="rainviewer", nominal_timestamp=timestamp,
            bounds=(-80.3, 25.6, -80.1, 25.8), grid_deg=0.02,
            height=10, width=10,
            cells=((4, 2, 10), (4, 7, right_intensity)),
            input_runs=2,
        )

    result = pysteps_preflight_module.evaluate_frames((
        frame(1_200, 10), frame(1_800, 100), frame(2_400, 10),
    ))
    assert result["translation_km_per_hour"] == 0.0
    assert result["heading_deg"] == 0.0
    assert result["centroid_translation_km_per_hour"] > 20
    assert result["verification_iou"] == 1.0


def test_shifted_iou_matches_set_reference(
    radar_frame_module, pysteps_preflight_module,
):
    origin = radar_frame_module.RadarFrame(
        source="opera", nominal_timestamp=1_200,
        bounds=(4.0, 50.0, 4.2, 50.2), grid_deg=0.02,
        height=10, width=10,
        cells=((0, 0, 5), (4, 4, 10), (9, 9, 20)), input_runs=3,
    )
    observed = radar_frame_module.RadarFrame(
        source="opera", nominal_timestamp=1_260,
        bounds=origin.bounds, grid_deg=origin.grid_deg,
        height=10, width=10,
        cells=((4, 5, 10), (9, 9, 20)), input_runs=2,
    )
    iou, overlap = pysteps_preflight_module._shifted_iou(
        origin, observed, 0, 1
    )
    assert overlap == 1
    assert iou == 1 / 3


def test_translation_match_reuses_unchanged_frame_pair(
    radar_frame_module, pysteps_preflight_module,
):
    first = _frame(radar_frame_module, 1_200, 2)
    second = _frame(radar_frame_module, 1_260, 3)
    pysteps_preflight_module._match_translation.cache_clear()
    pysteps_preflight_module._match_translation(first, second)
    pysteps_preflight_module._match_translation(first, second)
    info = pysteps_preflight_module._match_translation.cache_info()
    assert info.misses == 1
    assert info.hits == 1


def test_speed_above_legacy_limit_is_resolved_inside_bounded_search(
    radar_frame_module, pysteps_preflight_module,
):
    def wide_frame(timestamp, column):
        return radar_frame_module.RadarFrame(
            source="opera", nominal_timestamp=timestamp,
            bounds=(4.0, 50.0, 5.8, 50.4), grid_deg=0.02,
            height=20, width=90,
            cells=tuple(
                (row, col, 10)
                for row in range(5, 9)
                for col in range(column, column + 4)
            ),
            input_runs=4,
        )

    result = pysteps_preflight_module.evaluate_frames((
        wide_frame(1_200, 5),
        wide_frame(1_260, 35),
        wide_frame(1_320, 65),
    ))

    assert result["status"] == "ready"
    assert result["translation_km_per_hour"] > 180.0
    assert result["translation_censored"] is False
    assert result["search_window_exhaustive"] is False
    assert result["search_window_candidates"] <= 25 * 25
    assert result["translation_uncertainty"] == (
        "resolved_inside_compute_window"
    )


def test_search_boundary_returns_candidate_as_indeterminate(
    radar_frame_module, pysteps_preflight_module,
):
    origin = radar_frame_module.RadarFrame(
        source="opera", nominal_timestamp=1_200,
        bounds=(4.0, 50.0, 6.0, 50.4), grid_deg=0.02,
        height=20, width=100,
        cells=tuple(
            (row, column, 10)
            for row in range(5, 9)
            for column in range(10, 14)
        ),
        input_runs=4,
    )
    observed = radar_frame_module.RadarFrame(
        source=origin.source, nominal_timestamp=1_260,
        bounds=origin.bounds, grid_deg=origin.grid_deg,
        height=origin.height, width=origin.width,
        cells=(
            *tuple(
                (row, column, 10)
                for row in range(5, 9)
                for column in range(20, 24)
            ),
            (0, 78, 10), (2, 79, 10), (10, 80, 10),
            (12, 81, 10), (14, 82, 10),
        ),
        input_runs=6,
    )

    result = pysteps_preflight_module.evaluate_frames((origin, observed))

    assert result["status"] == "motion_indeterminate"
    assert result["translation_censored"] is True
    assert result["translation_km_per_hour"] is not None
    assert result["confidence"] == "indeterminate"
    assert result["reason"] == (
        "beste_match_op_rand_computationeel_zoekvenster"
    )
