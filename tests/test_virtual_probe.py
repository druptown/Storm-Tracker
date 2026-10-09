from types import SimpleNamespace


def _storm(**overrides):
    values = {
        "storm_id": "storm-a",
        "tracking_status": "bevestigd",
        "confidence": "Matig",
        "centroid_lat": 51.0,
        "centroid_lon": 4.0,
        "speed_kmh": 40.0,
        "heading_deg": 90.0,
        "velocity_east_kmh": 40.0,
        "velocity_north_kmh": 0.0,
        "uncertainty_km": 18.0,
        "motion_sample_count": 2,
        "radius_km": 5.0,
        "motion_to_target": lambda lat, lon: {
            "closest_pass_minutes": None,
            "passage_classification": None,
            "footprint_pass_distance_km": None,
            "approach_speed_kmh": None,
        },
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _gauge(
    lat=51.0,
    lon=4.18,
    rain_capable=True,
    timestamp=1_000.0,
    station_id="gauge-a",
):
    return SimpleNamespace(
        station_id=station_id,
        lat=lat,
        lon=lon,
        rain_capable=rain_capable,
        timestamp=timestamp,
        rain_mm=0.0,
        rain_60min=0.0,
    )


def test_history_counters_survive_manager_restart(virtual_probe_module):
    manager = virtual_probe_module.VirtualProbeManager(
        history={
            "completed": 7,
            "hits": 3,
            "misses": 2,
            "unverifiable": 1,
            "cancelled": 1,
            "recent_outcomes": [{"probe_id": "old"}],
        }
    )

    diagnostics = manager.diagnostics(now=1_000.0)

    assert diagnostics["completed_count"] == 7
    assert diagnostics["confirmed_count"] == 3
    assert diagnostics["dry_count"] == 2
    assert diagnostics["unverifiable_count"] == 1
    assert diagnostics["cancelled_count"] == 1
    assert diagnostics["recent_outcomes"] == [{"probe_id": "old"}]


def test_roaming_target_is_shared_across_regions_and_uses_best_candidate(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager()
    target = manager.choose_roaming_target(
        storms_by_engine={
            "region-1": [_storm(confidence="Hoog", uncertainty_km=2.0)],
            "region-2": [
                _storm(
                    storm_id="storm-it",
                    centroid_lat=45.0,
                    centroid_lon=9.0,
                    confidence="Matig",
                    uncertainty_km=25.0,
                )
            ],
        },
        observations_by_engine={"region-1": [], "region-2": []},
        now=1_000.0,
    )
    assert target["region_id"] == "region-2"
    assert target["storm_id"] == "storm-it"
    assert target["reason"] == "hoogste_kalibratiewaarde"


def test_roaming_target_preserves_extreme_finite_motion_without_speed_cap(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager()
    target = manager.choose_roaming_target(
        storms_by_engine={
            "region-1": [
                _storm(
                    speed_kmh=880.0,
                    velocity_east_kmh=880.0,
                    velocity_north_kmh=0.0,
                )
            ],
        },
        observations_by_engine={"region-1": []},
        now=1_000.0,
    )

    assert target is not None
    assert target["storm_id"] == "storm-a"
    assert -90.0 <= target["latitude"] <= 90.0
    assert -180.0 <= target["longitude"] < 180.0
    assert target["longitude"] > 9.0


def test_roaming_query_is_skipped_when_fixed_query_has_gauge_near_projection(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager()
    target = manager.choose_roaming_target(
        storms_by_engine={"region-1": [_storm()]},
        observations_by_engine={"region-1": [_gauge()]},
        now=1_000.0,
    )
    assert target is None


def test_low_confidence_two_frame_vector_is_used_for_learning(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager()
    samples, diagnostics = manager.update(
        storms_by_engine={
            "region-1": [_storm(confidence="Onvoldoende data")]
        },
        observations_by_engine={
            "region-1": [_gauge(lon=4.20)]
        },
        radar_sources_by_engine={
            "region-1": {"source": "rainviewer"}
        },
        now=1_000.0,
    )
    assert diagnostics["active_count"] == 1
    assert diagnostics["probes"][0]["motion_confidence"] == "Onvoldoende data"
    assert samples[0]["sample_type"] == "virtual_probe_forecast"


def test_repeated_updates_never_exceed_two_probes_per_storm(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager()
    observations = [
        _gauge(lon=4.16 + index * 0.06, station_id=f"gauge-{index}")
        for index in range(5)
    ]
    kwargs = {
        "storms_by_engine": {"region-1": [_storm()]},
        "observations_by_engine": {"region-1": observations},
        "radar_sources_by_engine": {"region-1": {"source": "kmi"}},
    }

    manager.update(**kwargs, now=1_000.0)
    _, diagnostics = manager.update(**kwargs, now=1_300.0)

    assert diagnostics["active_count"] == 2
    assert {
        probe["storm_id"] for probe in diagnostics["probes"]
    } == {"storm-a"}


def test_stale_storm_does_not_create_probe(virtual_probe_module):
    manager = virtual_probe_module.VirtualProbeManager()
    samples, diagnostics = manager.update(
        storms_by_engine={
            "region-1": [_storm(last_radar_timestamp=99.0)]
        },
        observations_by_engine={"region-1": [_gauge(timestamp=1_000.0)]},
        radar_sources_by_engine={
            "region-1": {"source": "rainviewer"}
        },
        now=1_000.0,
    )

    assert samples == []
    assert diagnostics["active_count"] == 0


def test_active_probe_is_cancelled_when_radar_frame_is_stale(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager([{
        "probe_id": "p-stale-radar",
        "storm_id": "storm-a",
        "region_id": "region-1",
        "station_id": "gauge-a",
        "latitude": 51.0,
        "longitude": 4.18,
        "created_at": 100.0,
        "expected_at": 1_300.0,
        "eta_minutes": 20.0,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Matig",
        "radar_source": "rainviewer",
    }])

    samples, diagnostics = manager.update(
        storms_by_engine={
            "region-1": [_storm(last_radar_timestamp=99.0)]
        },
        observations_by_engine={"region-1": []},
        radar_sources_by_engine={},
        now=1_000.0,
    )

    assert diagnostics["active_count"] == 0
    assert diagnostics["cancelled_count"] == 1
    assert samples[0]["own_status"] == "radarbeeld_verouderd"


def test_equal_eta_and_distance_candidates_sort_by_station_id(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager()
    samples, diagnostics = manager.update(
        storms_by_engine={"region-1": [_storm()]},
        observations_by_engine={"region-1": [
            _gauge(station_id="gauge-b"),
            _gauge(station_id="gauge-a"),
        ]},
        radar_sources_by_engine={"region-1": {"source": "rainviewer"}},
        now=1_000.0,
    )
    assert diagnostics["active_count"] == 1
    assert samples[0]["target_id"] == "netatmo:gauge-a"


def test_active_probe_gets_roaming_priority(virtual_probe_module):
    restored = [{
        "probe_id": "p1",
        "storm_id": "storm-old",
        "region_id": "region-2",
        "station_id": "station-it",
        "latitude": 45.2,
        "longitude": 9.3,
        "created_at": 900.0,
        "expected_at": 1_300.0,
        "eta_minutes": 6.7,
        "passage": "raak",
        "pass_distance_km": 1.0,
        "motion_confidence": "Matig",
        "radar_source": "rainviewer",
    }]
    manager = virtual_probe_module.VirtualProbeManager(restored)
    target = manager.choose_roaming_target(
        storms_by_engine={"region-1": [_storm()]},
        observations_by_engine={"region-1": []},
        now=1_000.0,
    )
    assert target["region_id"] == "region-2"
    assert target["storm_id"] == "storm-old"
    assert target["reason"] == "lopende_verificatie"
    diagnostics = manager.diagnostics(now=1_000.0)
    assert diagnostics["status_text"] == "1 virtuele regenverkenner(s) actief"
    assert diagnostics["probes"][0]["status_text"] in {
        "Onderweg naar controlepunt", "Wacht op regenmeting",
    }
    assert "Controleert bui storm-old" in diagnostics["probes"][0]["summary"]


def test_missing_station_expires_as_unverifiable_without_dry_penalty(
    virtual_probe_module,
):
    restored = [{
        "probe_id": "p-missing",
        "storm_id": "storm-old",
        "region_id": "region-1",
        "station_id": "station-missing",
        "latitude": 51.2,
        "longitude": 4.3,
        "created_at": 1_000.0,
        "expected_at": 1_300.0,
        "eta_minutes": 5.0,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Laag",
        "radar_source": "rainviewer",
    }]
    manager = virtual_probe_module.VirtualProbeManager(restored)

    samples, diagnostics = manager.update(
        storms_by_engine={},
        observations_by_engine={"region-1": []},
        radar_sources_by_engine={},
        now=1_300.0 + 30 * 60 + 1,
    )

    assert diagnostics["active_count"] == 0
    assert diagnostics["completed_count"] == 1
    assert diagnostics["dry_count"] == 0
    assert diagnostics["unverifiable_count"] == 1
    assert samples[0]["own_status"] == "niet_verifieerbaar"
    assert samples[0]["snapshot"]["rain_live_mm_h"] is None


def test_stale_station_gets_grace_period_before_unverifiable(
    virtual_probe_module,
):
    restored = [{
        "probe_id": "p-stale",
        "storm_id": "storm-old",
        "region_id": "region-1",
        "station_id": "station-stale",
        "latitude": 51.2,
        "longitude": 4.3,
        "created_at": 1_000.0,
        "expected_at": 1_300.0,
        "eta_minutes": 5.0,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Laag",
        "radar_source": "rainviewer",
    }]
    manager = virtual_probe_module.VirtualProbeManager(restored)

    samples, diagnostics = manager.update(
        storms_by_engine={},
        observations_by_engine={
            "region-1": [_gauge(
                station_id="station-stale",
                timestamp=1_000.0,
            )],
        },
        radar_sources_by_engine={},
        now=1_300.0 + 29 * 60,
    )

    assert samples == []
    assert diagnostics["active_count"] == 1
    assert diagnostics["unverifiable_count"] == 0


def test_probe_is_cancelled_when_region_contains_other_storm(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager([{
        "probe_id": "p-scotland",
        "storm_id": "storm-scotland",
        "region_id": "region-2",
        "station_id": "station-scotland",
        "latitude": 55.6,
        "longitude": -2.8,
        "created_at": 1_000.0,
        "expected_at": 2_000.0,
        "eta_minutes": 16.7,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Matig",
        "radar_source": "met_office_radar",
    }])

    samples, diagnostics = manager.update(
        storms_by_engine={
            "region-2": [_storm(storm_id="storm-italy")]
        },
        observations_by_engine={"region-2": []},
        radar_sources_by_engine={},
        now=1_100.0,
    )

    assert diagnostics["active_count"] == 0
    assert samples[0]["own_status"] == "stormobject_verdwenen"
    assert diagnostics["cancelled_count"] == 1


def test_probe_is_cancelled_after_fresh_empty_radar_scan(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager([{
        "probe_id": "p-gone",
        "storm_id": "storm-gone",
        "region_id": "region-1",
        "station_id": "station-a",
        "latitude": 51.0,
        "longitude": 4.2,
        "created_at": 900.0,
        "expected_at": 1_300.0,
        "eta_minutes": 6.7,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Matig",
        "radar_source": "kmi",
    }])

    samples, diagnostics = manager.update(
        storms_by_engine={"region-1": []},
        observations_by_engine={"region-1": []},
        radar_sources_by_engine={"region-1": {
            "source": "kmi",
            "age_seconds": 300,
        }},
        now=1_000.0,
    )

    assert diagnostics["active_count"] == 0
    assert diagnostics["cancelled_count"] == 1
    assert samples[0]["own_status"] == "stormobject_verdwenen"


def test_probe_survives_empty_region_when_radar_is_stale(
    virtual_probe_module,
):
    manager = virtual_probe_module.VirtualProbeManager([{
        "probe_id": "p-provider-gap",
        "storm_id": "storm-a",
        "region_id": "region-1",
        "station_id": "station-a",
        "latitude": 51.0,
        "longitude": 4.2,
        "created_at": 900.0,
        "expected_at": 1_300.0,
        "eta_minutes": 6.7,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Matig",
        "radar_source": "kmi",
    }])

    samples, diagnostics = manager.update(
        storms_by_engine={"region-1": []},
        observations_by_engine={"region-1": []},
        radar_sources_by_engine={"region-1": {
            "source": "kmi",
            "age_seconds": 1_000,
        }},
        now=1_000.0,
    )

    assert samples == []
    assert diagnostics["active_count"] == 1


def test_probe_is_cancelled_after_storm_turns_away(virtual_probe_module):
    manager = virtual_probe_module.VirtualProbeManager([{
        "probe_id": "p-old-track",
        "storm_id": "storm-a",
        "region_id": "region-1",
        "station_id": "station-west",
        "latitude": 51.0,
        "longitude": 3.8,
        "created_at": 1_000.0,
        "expected_at": 2_000.0,
        "eta_minutes": 16.7,
        "passage": "raak",
        "pass_distance_km": 0.0,
        "motion_confidence": "Matig",
        "radar_source": "rainviewer",
    }])

    samples, diagnostics = manager.update(
        storms_by_engine={"region-1": [_storm(
            centroid_lon=4.0,
            heading_deg=90.0,
            velocity_east_kmh=40.0,
            velocity_north_kmh=0.0,
        )]},
        observations_by_engine={"region-1": []},
        radar_sources_by_engine={},
        now=1_100.0,
    )

    assert diagnostics["active_count"] == 0
    assert diagnostics["cancelled_count"] == 1
    assert samples[0]["own_status"] == "traject_gewijzigd"
