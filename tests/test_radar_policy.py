from types import SimpleNamespace


def _select(radar_policy_module, **kwargs):
    return radar_policy_module.select_radar_source(**kwargs)


def test_healthy_opera_is_primary(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=True, opera_healthy=True, rainviewer_configured=True
    )
    assert decision.source == "opera"


def test_rainviewer_is_fallback_for_unhealthy_opera(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=True, opera_healthy=False, rainviewer_configured=True
    )
    assert decision.source == "rainviewer"


def test_rainviewer_is_primary_outside_opera_coverage(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=False, opera_healthy=False, rainviewer_configured=True
    )
    assert decision.source == "rainviewer"


def test_no_source_when_none_available(radar_policy_module):
    decision = _select(radar_policy_module,
        opera_configured=False, opera_healthy=False, rainviewer_configured=False
    )
    assert decision.source is None


def _obs(*, lat=51.0, lon=4.5, timestamp=1_000.0, quality=None,
         footprint_points=()):
    return SimpleNamespace(
        lat=lat, lon=lon, timestamp=timestamp, quality=quality,
        footprint_points=footprint_points,
    )


def test_opera_high_quality_is_accepted_without_confirmation(radar_policy_module):
    result = radar_policy_module.verify_opera_observations(
        [_obs(quality=0.75)], []
    )
    assert len(result.accepted) == 1
    assert result.high_quality == 1
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


def test_opera_low_quality_rejects_distant_or_stale_confirmation(radar_policy_module):
    opera = _obs(lat=50.68, lon=5.80, timestamp=10_000.0, quality=0.0)
    distant = _obs(lat=52.60, lon=3.39, timestamp=10_000.0)
    stale = _obs(lat=50.68, lon=5.80, timestamp=1_000.0)
    result = radar_policy_module.verify_opera_observations(
        [opera], [distant, stale]
    )
    assert result.accepted == ()
    assert result.rejected == 1


def test_opera_uses_actual_footprint_not_only_distant_centroid(radar_policy_module):
    opera = _obs(
        lat=48.40, lon=-3.60, timestamp=1_000.0, quality=0.1,
        footprint_points=((49.42, -3.12), (49.45, -3.08)),
    )
    rainviewer = _obs(lat=49.43, lon=-3.10, timestamp=1_060.0)
    result = radar_policy_module.verify_opera_observations([opera], [rainviewer])
    assert result.accepted == (opera,)
    assert result.corroborated == 1


def test_opera_does_not_treat_whole_area_as_a_centroid_circle(radar_policy_module):
    opera = _obs(
        lat=48.40, lon=-3.60, timestamp=1_000.0, quality=0.1,
        footprint_points=((48.40, -3.60), (48.45, -3.55)),
    )
    rainviewer = _obs(lat=49.40, lon=-3.60, timestamp=1_060.0)
    result = radar_policy_module.verify_opera_observations([opera], [rainviewer])
    assert result.accepted == ()
    assert result.rejected == 1
