from types import SimpleNamespace


def _select(radar_policy_module, **kwargs):
    return radar_policy_module.select_radar_source(**kwargs)


def test_healthy_opera_is_primary(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=True, opera_healthy=True, rainviewer_configured=True,
        rainviewer_healthy=True
    )
    assert decision.source == "opera"


def test_rainviewer_is_fallback_for_unhealthy_opera(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=True, opera_healthy=False, rainviewer_configured=True,
        rainviewer_healthy=True
    )
    assert decision.source == "rainviewer"


def test_rainviewer_is_primary_outside_opera_coverage(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=False, opera_healthy=False, rainviewer_configured=True,
        rainviewer_healthy=True
    )
    assert decision.source == "rainviewer"


def test_no_source_when_none_available(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=False, opera_healthy=False, rainviewer_configured=False,
        rainviewer_healthy=False
    )
    assert decision.source is None


def test_stale_rainviewer_is_not_selected(radar_policy_module):
    decision = _select(
        radar_policy_module,
        opera_configured=True,
        opera_healthy=False,
        rainviewer_configured=True,
        rainviewer_healthy=False,
    )
    assert decision.source is None
    assert "stale" in decision.reason


def _obs(*, lat=51.0, lon=4.5, timestamp=1_000.0, quality=None,
         footprint_points=(), source="rainviewer", intensity=2,
         mean_dbz=None, max_dbz=None, area_km2=None):
    return SimpleNamespace(
        lat=lat, lon=lon, timestamp=timestamp, quality=quality,
        footprint_points=footprint_points, source=source, intensity=intensity,
        mean_dbz=mean_dbz, max_dbz=max_dbz, area_km2=area_km2,
    )


def test_corroboration_filter_rejects_intensity_one_basemaps(
    radar_policy_module,
):
    observations = [
        _obs(source="kmi", intensity=1),
        _obs(source="kmi", intensity=2),
        _obs(source="knmi", intensity=1),
        _obs(source="knmi", intensity=2),
        _obs(source="rainviewer", intensity=1),
        _obs(source="rainviewer", intensity=2),
    ]

    usable = radar_policy_module.usable_corroborating_observations(observations)

    assert usable == (observations[1], observations[3], observations[5])


def test_corroboration_source_counts_reports_actual_usable_sources(
    radar_policy_module,
):
    observations = [
        _obs(source="kmi"),
        _obs(source="kmi"),
        _obs(source="knmi"),
        _obs(source="rainviewer"),
        _obs(source="unknown"),
    ]

    assert radar_policy_module.corroboration_source_counts(observations) == {
        "kmi": 2,
        "knmi": 1,
        "rainviewer": 1,
    }


def test_opera_high_quality_is_accepted_without_confirmation(radar_policy_module):
    result = radar_policy_module.verify_opera_observations(
        [_obs(quality=0.75)], []
    )
    assert len(result.accepted) == 1
    assert result.high_quality == 1
    assert result.structured_echo == 0
    assert result.corroborated == 0
    assert result.rejected == 0


def test_opera_low_quality_is_rejected_without_confirmation(radar_policy_module):
    result = radar_policy_module.verify_opera_observations(
        [_obs(quality=0.0)], []
    )
    assert result.accepted == ()
    assert result.rejected == 1


def test_opera_low_quality_is_accepted_near_recent_national_radar(radar_policy_module):
    opera = _obs(lat=52.60, lon=3.39, timestamp=1_000.0, quality=0.0)
    kmi = _obs(lat=52.62, lon=3.42, timestamp=1_120.0)
    result = radar_policy_module.verify_opera_observations([opera], [kmi])
    assert result.accepted == (opera,)
    assert result.corroborated == 1
    assert result.rejected == 0


def test_opera_low_quality_is_accepted_near_recent_rainviewer(radar_policy_module):
    opera = _obs(lat=48.37, lon=-3.65, timestamp=1_000.0, quality=0.1)
    rainviewer = _obs(lat=48.40, lon=-3.62, timestamp=1_120.0)
    result = radar_policy_module.verify_opera_observations(
        [opera], [rainviewer]
    )
    assert result.accepted == (opera,)
    assert result.corroborated == 1


def test_opera_low_quality_rejects_confirmation_outside_tight_radius(
    radar_policy_module,
):
    opera = _obs(lat=51.0, lon=4.0, timestamp=1_000.0, quality=0.1)
    rainviewer = _obs(lat=51.16, lon=4.0, timestamp=1_120.0, intensity=2)

    result = radar_policy_module.verify_opera_observations([opera], [rainviewer])

    assert result.accepted == ()
    assert result.rejected == 1


def test_opera_low_quality_rejects_distant_or_stale_confirmation(radar_policy_module):
    opera = _obs(lat=50.68, lon=5.80, timestamp=10_000.0, quality=0.0)
    distant = _obs(lat=52.60, lon=3.39, timestamp=10_000.0)
    stale = _obs(lat=50.68, lon=5.80, timestamp=1_000.0)
    result = radar_policy_module.verify_opera_observations(
        [opera], [distant, stale]
    )
    assert result.accepted == ()
    assert result.rejected == 1


def test_opera_strong_structured_echo_is_accepted_with_low_quality(
    radar_policy_module,
):
    opera = _obs(
        quality=0.01, mean_dbz=31.0, max_dbz=52.0, area_km2=250.0
    )

    result = radar_policy_module.verify_opera_observations([opera], [])

    assert len(result.accepted) == 1
    assert result.structured_echo == 1
    assert result.corroborated == 0
    assert result.rejected == 0


def test_opera_confirmed_structured_echo_is_accepted(
    radar_policy_module,
):
    opera = _obs(
        quality=0.01, mean_dbz=31.0, max_dbz=52.0, area_km2=250.0
    )
    rainviewer = _obs(lat=51.02, lon=4.52, timestamp=1_060.0, intensity=2)

    result = radar_policy_module.verify_opera_observations(
        [opera], [rainviewer]
    )

    assert result.accepted == (opera,)
    assert result.structured_echo == 1
    assert result.corroborated == 0


def test_opera_weak_broad_echo_still_requires_corroboration(radar_policy_module):
    opera = _obs(
        quality=0.01, mean_dbz=14.0, max_dbz=47.5, area_km2=1174.0
    )

    result = radar_policy_module.verify_opera_observations([opera], [])

    assert result.accepted == ()
    assert result.structured_echo == 0
    assert result.rejected == 1


def test_opera_uses_actual_footprint_not_only_distant_centroid(radar_policy_module):
    opera = _obs(
        lat=48.40, lon=-3.60, timestamp=1_000.0, quality=0.1,
        footprint_points=((49.42, -3.12), (49.45, -3.08)),
    )
    rainviewer = _obs(lat=49.43, lon=-3.10, timestamp=1_060.0)
    result = radar_policy_module.verify_opera_observations([opera], [rainviewer])
    assert len(result.accepted) == 1
    assert result.accepted[0].footprint_points == opera.footprint_points
    assert result.corroborated == 1


def test_opera_large_footprint_is_clipped_to_confirmed_area(radar_policy_module):
    footprint = ((49.0, 2.0), (49.1, 2.1), (50.0, 3.0), (50.1, 3.1))
    opera = _obs(
        lat=49.55, lon=2.55, timestamp=1_000.0, quality=0.01,
        footprint_points=footprint, area_km2=400.0,
    )
    rainviewer = _obs(lat=49.05, lon=2.05, timestamp=1_060.0)

    result = radar_policy_module.verify_opera_observations(
        [opera], [rainviewer], radius_km=12.0
    )

    clipped = result.accepted[0]
    assert clipped.footprint_points == footprint[:2]
    assert clipped.area_km2 == 200.0
    assert clipped.lat == 49.05
    assert clipped.lon == 2.05


def test_opera_weak_echo_uses_enclosed_national_radar_footprint(
    radar_policy_module,
):
    """Corroboration works when DPC lies inside a broad OPERA footprint."""
    opera_footprint = (
        (44.8, 12.0), (44.8, 16.0), (46.0, 16.0),
        (46.0, 12.0), (44.8, 12.0),
    )
    dpc_footprint = (
        (45.15, 13.05), (45.15, 13.25), (45.30, 13.25),
        (45.30, 13.05), (45.15, 13.05),
    )
    opera = _obs(
        lat=45.4, lon=14.0, timestamp=1_000.0, quality=0.0,
        footprint_points=opera_footprint, source="opera",
        mean_dbz=12.0, max_dbz=20.0, area_km2=2_479.0,
    )
    dpc = _obs(
        lat=45.22, lon=13.15, timestamp=1_060.0,
        footprint_points=dpc_footprint, source="dpc_radar", intensity=2,
    )

    result = radar_policy_module.verify_opera_observations([opera], [dpc])

    assert len(result.accepted) == 1
    assert result.accepted[0].footprint_points == dpc_footprint
    assert result.corroborated == 1
    assert result.rejected == 0


def test_opera_does_not_treat_whole_area_as_a_centroid_circle(radar_policy_module):
    opera = _obs(
        lat=48.40, lon=-3.60, timestamp=1_000.0, quality=0.1,
        footprint_points=((48.40, -3.60), (48.45, -3.55)),
    )
    rainviewer = _obs(lat=49.40, lon=-3.60, timestamp=1_060.0)
    result = radar_policy_module.verify_opera_observations([opera], [rainviewer])
    assert result.accepted == ()
    assert result.rejected == 1
