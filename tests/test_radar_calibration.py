from types import SimpleNamespace


def _obs(lat, lon, *, footprint=()):
    return SimpleNamespace(lat=lat, lon=lon, footprint_points=footprint)


def test_observer_scores_overlap_and_never_changes_filtering(radar_calibration_module):
    observer = radar_calibration_module.RadarCalibrationObserver(grid_deg=0.1)
    primary = [_obs(51.01, 4.01), _obs(51.21, 4.21)]
    reference = [_obs(51.02, 4.02), _obs(51.31, 4.31)]

    result = observer.observe(
        primary, reference, reference_source="kmi_image",
        reference_timestamp=1_000, now=1_000,
    )

    assert result.overlap_cells == 1
    assert result.false_positive_cells == 1
    assert result.missed_cells == 1
    assert result.precision == 0.5
    assert result.recall == 0.5
    assert observer.diagnostics()["changes_filtering"] is False


def test_observer_accepts_fresh_dry_reference_and_rejects_stale_frame(
    radar_calibration_module,
):
    observer = radar_calibration_module.RadarCalibrationObserver()
    dry = observer.observe(
        [_obs(51, 4)], [], reference_source="kmi_image",
        reference_timestamp=1_000, now=1_000,
    )
    assert dry.false_positive_cells == 1
    assert dry.reference_cells == 0
    assert dry.recall is None

    stale = observer.observe(
        [], [], reference_source="kmi_image",
        reference_timestamp=1_000, now=2_000,
    )
    assert stale is None
    assert observer.diagnostics()["samples"] == 1


def test_observer_limits_both_sources_to_common_evaluation_area(
    radar_calibration_module,
):
    observer = radar_calibration_module.RadarCalibrationObserver(grid_deg=0.1)
    result = observer.observe(
        [_obs(51.0, 4.0)],
        [_obs(51.0, 4.0), _obs(48.0, 1.0)],
        reference_source="kmi_image",
        reference_timestamp=1_000,
        evaluation_center=(51.0, 4.0),
        evaluation_radius_km=100,
        now=1_000,
    )
    assert result.primary_cells == 1
    assert result.reference_cells == 1
    assert result.f1_score == 1.0


def test_history_matches_only_exact_nominal_minute(radar_calibration_module):
    observer = radar_calibration_module.RadarCalibrationObserver()
    observer.record_primary_frame([_obs(51, 4)], 1_020)
    assert observer.record_reference_frame(
        [_obs(51, 4)], source="kmi_image", timestamp=1_080
    ) == 0
    assert observer.diagnostics()["samples"] == 0
    assert observer.record_reference_frame(
        [_obs(51, 4)], source="kmi_image", timestamp=1_020
    ) == 1
    diagnostics = observer.diagnostics()
    assert diagnostics["samples"] == 1
    assert diagnostics["latest"]["f1_score"] == 1.0
    assert diagnostics["synchronisatie"] == "exacte_nominale_minuut"


def test_history_matches_when_reference_arrives_first(radar_calibration_module):
    observer = radar_calibration_module.RadarCalibrationObserver()
    observer.record_reference_frame([], source="kmi_image", timestamp=1_200)
    assert observer.record_primary_frame([_obs(51, 4)], 1_200) == 1
    latest = observer.diagnostics()["latest"]
    assert latest["missed_cells"] == 1
    assert latest["provider_pair"] == "kmi_image<->opera"


def test_multi_provider_frames_are_compared_per_region(radar_calibration_module):
    observer = radar_calibration_module.RadarCalibrationObserver(grid_deg=0.1)
    observer.record_frame(
        [_obs(51.01, 4.01)], source="kmi", timestamp=1_200,
        region_id="region-1",
    )
    assert observer.record_frame(
        [_obs(51.02, 4.02)], source="rainviewer", timestamp=1_200,
        region_id="region-1",
    ) == 1
    # Hetzelfde tijdstip in een andere regio mag nooit worden gekruist.
    assert observer.record_frame(
        [_obs(51.02, 4.02)], source="opera", timestamp=1_200,
        region_id="region-2",
    ) == 0
    diagnostics = observer.diagnostics()
    assert diagnostics["samples"] == 1
    assert diagnostics["latest"]["region_id"] == "region-1"
    assert diagnostics["provider_pairs"]["kmi<->rainviewer"] == {
        "samples": 1, "mean_f1_score": 1.0,
    }


def test_three_sources_create_each_pair_once(radar_calibration_module):
    observer = radar_calibration_module.RadarCalibrationObserver()
    for source in ("kmi", "opera", "rainviewer"):
        observer.record_frame(
            [_obs(51, 4)], source=source, timestamp=1_200,
            region_id="region-1",
        )
    diagnostics = observer.diagnostics()
    assert diagnostics["samples"] == 3
    assert set(diagnostics["provider_pairs"]) == {
        "kmi<->opera", "kmi<->rainviewer", "opera<->rainviewer",
    }


def test_collection_batch_keeps_dry_frames_and_grid_intensity(
    radar_calibration_module,
):
    wet = _obs(51.01, 4.01)
    wet.intensity = 6
    wet.quality = 0.8
    observer = radar_calibration_module.RadarCalibrationObserver(grid_deg=0.1)
    observer.record_frame(
        [], source="kmi", timestamp=1_200, region_id="region",
    )
    observer.record_frame(
        [wet], source="opera", timestamp=1_200, region_id="region",
    )
    batch = observer.drain_collection_batch()
    assert len(batch["frames"]) == 2
    dry = next(frame for frame in batch["frames"] if frame[1] == "kmi")
    assert dry[6:8] == (0, 0)
    opera = next(frame for frame in batch["frames"] if frame[1] == "opera")
    assert opera[13] == ((510, 40, 6.0, 0.8, 1),)
    assert opera[12] == 1.0
    assert len(batch["comparisons"]) == 1
    assert observer.drain_collection_batch() == {"frames": (), "comparisons": ()}


def test_frames_below_shared_coverage_threshold_are_not_compared(
    radar_calibration_module,
):
    observer = radar_calibration_module.RadarCalibrationObserver(grid_deg=0.1)
    center = (45.47, 9.19)
    radius = 250.0
    observer.record_frame(
        [],
        source="kmi",
        timestamp=1_200,
        region_id="milan",
        evaluation_center=center,
        evaluation_radius_km=radius,
        coverage_bbox=(-2.5, 46.5, 10.5, 53.0),
    )
    matched = observer.record_frame(
        [_obs(45.5, 9.2)],
        source="dpc_radar",
        timestamp=1_200,
        region_id="milan",
        evaluation_center=center,
        evaluation_radius_km=radius,
        coverage_bbox=(4.5, 35.0, 20.5, 48.0),
    )
    assert matched == 0
    assert observer.diagnostics()["samples"] == 0


def test_comparison_records_shared_coverage_fraction(
    radar_calibration_module,
):
    observer = radar_calibration_module.RadarCalibrationObserver(grid_deg=0.1)
    observer.record_frame(
        [_obs(51.0, 4.0)],
        source="kmi",
        timestamp=1_200,
        region_id="belgium",
        evaluation_center=(51.0, 4.0),
        evaluation_radius_km=100.0,
        coverage_bbox=(-2.5, 46.5, 10.5, 53.0),
    )
    matched = observer.record_frame(
        [_obs(51.0, 4.0)],
        source="rainviewer",
        timestamp=1_200,
        region_id="belgium",
        evaluation_center=(51.0, 4.0),
        evaluation_radius_km=100.0,
        coverage_bbox=(3.0, 50.0, 5.0, 52.0),
    )
    assert matched == 1
    latest = observer.diagnostics()["latest"]
    assert latest["shared_coverage_fraction"] >= 0.6
