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
    assert observer.diagnostics()["latest"]["false_positive_cells"] == 1
