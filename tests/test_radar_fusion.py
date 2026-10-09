def _radar(observation_module, lat, lon, timestamp, source):
    return observation_module.Observation(
        observation_module.ObservationType.RADAR,
        lat,
        lon,
        timestamp,
        intensity=3,
        source=source,
    )


def test_noaa_fills_rainviewer_gap_without_losing_primary(
    observation_module, radar_fusion_module
):
    primary = [_radar(observation_module, 41.5, -75.0, 950.0, "rainviewer")]
    noaa = [_radar(
        observation_module, 40.7128, -74.0060, 960.0,
        "noaa_enterprise_rainrate",
    )]

    fused, diagnostic = radar_fusion_module.fuse_radar_observations(
        primary, (noaa,), now=1_000.0
    )

    assert len(fused) == 2
    assert {item.source for item in fused} == {
        "rainviewer", "noaa_enterprise_rainrate"
    }
    assert {item.timestamp for item in fused} == {950.0, 960.0}
    assert fused[0] is primary[0]
    assert fused[1] is noaa[0]
    assert diagnostic["active"] is True
    assert diagnostic["supplemental_observations"] == 1
    assert diagnostic["analysis_timestamp"] == 960.0
    assert diagnostic["timestamp_policy"] == (
        "preserve_source_product_time_v1"
    )


def test_fusion_keeps_1200_and_1204_as_distinct_product_times(
    observation_module, radar_fusion_module
):
    noon = 1_800_000_000.0
    rainviewer = _radar(
        observation_module, 41.5, -75.0, noon, "rainviewer"
    )
    noaa = _radar(
        observation_module,
        40.7128,
        -74.0060,
        noon + 4 * 60,
        "noaa_enterprise_rainrate",
    )

    fused, diagnostic = radar_fusion_module.fuse_radar_observations(
        [rainviewer], ([noaa],), now=noon + 5 * 60
    )

    assert [(item.source, item.timestamp) for item in fused] == [
        ("rainviewer", noon),
        ("noaa_enterprise_rainrate", noon + 4 * 60),
    ]
    assert diagnostic["analysis_timestamp"] == noon + 4 * 60
    assert diagnostic["fusion_evaluated_at"] == noon + 5 * 60
    assert diagnostic["source_product_timestamps"] == {
        "noaa_enterprise_rainrate": [noon + 4 * 60],
        "rainviewer": [noon],
    }
    assert diagnostic["contributing_product_timestamps"] == {
        "noaa_enterprise_rainrate": [noon + 4 * 60],
        "rainviewer": [noon],
    }
    assert "canonical_timestamp" not in diagnostic


def test_overlapping_noaa_echo_is_suppressed(
    observation_module, radar_fusion_module
):
    primary = [_radar(observation_module, 40.7128, -74.0060, 950.0, "rainviewer")]
    noaa = [_radar(
        observation_module, 40.72, -74.01, 960.0,
        "noaa_enterprise_rainrate",
    )]

    fused, diagnostic = radar_fusion_module.fuse_radar_observations(
        primary, (noaa,), now=1_000.0
    )

    assert len(fused) == 1
    assert fused[0].source == "rainviewer"
    assert fused[0].timestamp == 950.0
    assert diagnostic["duplicates_suppressed"] == 1
    assert diagnostic["analysis_timestamp"] == 960.0
    assert diagnostic["source_product_timestamps"] == {
        "noaa_enterprise_rainrate": [960.0],
        "rainviewer": [950.0],
    }
    assert diagnostic["contributing_product_timestamps"] == {
        "rainviewer": [950.0],
    }


def test_fusion_falls_back_to_each_available_source(
    observation_module, radar_fusion_module
):
    rainviewer = [_radar(
        observation_module, 40.7, -74.0, 950.0, "rainviewer"
    )]
    noaa = [_radar(
        observation_module, 40.7, -74.0, 950.0,
        "noaa_enterprise_rainrate",
    )]

    primary_only, primary_diagnostic = (
        radar_fusion_module.fuse_radar_observations(
            rainviewer, ([],), now=1_000.0
        )
    )
    noaa_only, noaa_diagnostic = radar_fusion_module.fuse_radar_observations(
        [], (noaa,), now=1_000.0
    )

    assert [item.source for item in primary_only] == ["rainviewer"]
    assert primary_diagnostic["active"] is False
    assert [item.source for item in noaa_only] == [
        "noaa_enterprise_rainrate"
    ]
    assert noaa_diagnostic["active"] is False
