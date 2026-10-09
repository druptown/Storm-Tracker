"""Tests voor locatiegerichte Netatmo-regenbevestiging."""
from types import SimpleNamespace

import pytest


def _station(
    lat, lon, rain, timestamp=1_000.0, station_id="station",
    source="netatmo",
):
    return SimpleNamespace(
        lat=lat,
        lon=lon,
        rain_mm=rain,
        timestamp=timestamp,
        station_id=station_id,
        source=source,
    )


def test_two_wet_stations_locally_confirm_rain(netatmo_local_module):
    result = netatmo_local_module.build_local_netatmo_summary(
        [
            _station(51.001, 4.001, 0.8, station_id="near-1"),
            _station(51.02, 4.02, 0.4, station_id="near-2"),
            _station(51.03, 4.03, 0.0, station_id="dry-1"),
        ],
        51.0,
        4.0,
        now=1_030.0,
    )

    assert result["netatmo_local_status"] == "regen_bevestigd"
    assert result["netatmo_local_confirmed"] is True
    assert result["netatmo_local_raining_stations"] == 2
    assert result["netatmo_local_max_rain_mm_h"] == 0.8
    assert result["netatmo_data_age_seconds"] == 30.0
    assert result["netatmo_nearest_station_id"] == "near-1"
    assert result["netatmo_nearest_rain_station_id"] == "near-1"


def test_one_wet_station_is_only_an_indication(netatmo_local_module):
    result = netatmo_local_module.build_local_netatmo_summary(
        [_station(51.001, 4.001, 0.5)],
        51.0,
        4.0,
        now=1_000.0,
    )

    assert result["netatmo_local_status"] == "regen_waargenomen"
    assert result["netatmo_local_confirmed"] is False
    assert result["ground_precipitation_observed"] is True


def test_two_synoptic_stations_confirm_with_synoptic_basis(
    netatmo_local_module,
):
    observations = [
        _station(
            station_id=f"synoptic:K{index}",
            lat=29.7604 + index * 0.01,
            lon=-95.3698,
            rain=0.4,
            source="synoptic",
        )
        for index in range(2)
    ]

    result = netatmo_local_module.build_local_netatmo_summary(
        observations, 29.7604, -95.3698, now=1_000.0
    )

    assert result["netatmo_local_confirmed"] is True
    assert result["ground_sources"] == ["synoptic"]
    assert result["ground_confirmation_basis"] == "synoptic_grondmeting"


def test_stale_or_wrong_system_stations_do_not_confirm(netatmo_local_module):
    observations = [
        _station(40.7128, -74.006, 1.0, timestamp=1_000, station_id="a"),
        _station(40.7130, -74.006, 1.0, timestamp=1_000, station_id="b"),
    ]
    remote_storm = {
        "storm_id": "remote",
        "footprint": [
            (41.0, -75.0), (41.1, -75.0), (41.1, -74.9), (41.0, -74.9),
        ],
    }
    wrong_system = netatmo_local_module.build_local_netatmo_summary(
        observations, 40.7128, -74.006, storms=[remote_storm], now=1_010
    )
    stale = netatmo_local_module.build_local_netatmo_summary(
        observations, 40.7128, -74.006, now=4_000
    )

    assert wrong_system["ground_confirmed"] is False
    assert wrong_system["ground_precipitation_observed"] is True
    assert wrong_system["ground_status"] == "regen_waargenomen"
    assert stale["ground_confirmed"] is False
    assert stale["ground_stale_station_count"] == 1
    assert stale["ground_raw_stale_observation_count"] == 2


def test_all_same_time_components_of_one_storm_can_confirm(
    netatmo_local_module,
):
    timestamp = 1_000.0
    storm = SimpleNamespace(
        storm_id="merged-system",
        radar_system_frames={
            "west": SimpleNamespace(
                timestamp=timestamp,
                footprint_points=(
                    (50.98, 3.98), (50.98, 4.02),
                    (51.02, 4.02), (51.02, 3.98),
                ),
            ),
            "east": SimpleNamespace(
                timestamp=timestamp,
                footprint_points=(
                    (50.98, 4.98), (50.98, 5.02),
                    (51.02, 5.02), (51.02, 4.98),
                ),
            ),
        },
    )
    observations = [
        _station(
            51.0, 5.0, 0.8, timestamp=timestamp, station_id="east-1"
        ),
        _station(
            51.01, 5.0, 0.9, timestamp=timestamp, station_id="east-2"
        ),
    ]

    result = netatmo_local_module.build_local_netatmo_summary(
        observations,
        51.0,
        5.0,
        storms=[storm],
        now=timestamp,
    )

    assert result["ground_confirmed"] is True
    assert result["ground_confirmed_storm_id"] == "merged-system"
    assert result["ground_system_raining_station_count"] == 2


def test_separate_components_do_not_create_a_wet_bridge(
    netatmo_local_module,
):
    timestamp = 1_000.0
    storm = {
        "storm_id": "split-system",
        "footprints": [
            [
                (50.98, 3.98), (50.98, 4.02),
                (51.02, 4.02), (51.02, 3.98),
            ],
            [
                (50.98, 4.98), (50.98, 5.02),
                (51.02, 5.02), (51.02, 4.98),
            ],
        ],
        "footprint_timestamp": timestamp,
    }
    observations = [
        _station(
            51.0, 4.5, 0.8, timestamp=timestamp, station_id="gap-1"
        ),
        _station(
            51.01, 4.5, 0.9, timestamp=timestamp, station_id="gap-2"
        ),
    ]

    result = netatmo_local_module.build_local_netatmo_summary(
        observations,
        51.0,
        4.5,
        storms=[storm],
        now=timestamp,
    )

    assert result["ground_precipitation_observed"] is True
    assert result["ground_confirmed"] is False
    assert result["ground_system_raining_station_count"] == 0


def test_nearest_wet_station_outside_radius_is_diagnostic_only(
    netatmo_local_module,
):
    result = netatmo_local_module.build_local_netatmo_summary(
        [_station(51.2, 4.0, 1.2)],
        51.0,
        4.0,
        now=1_000.0,
    )

    assert result["netatmo_local_status"] == "geen_lokale_stations"
    assert result["netatmo_local_raining_stations"] == 0
    assert result["netatmo_nearest_rain_distance_km"] > 20
    assert result["netatmo_nearest_rain_mm_h"] == 1.2


def test_two_wet_stations_on_different_storms_do_not_confirm(
    netatmo_local_module,
):
    observations = [
        _station(51.0, 3.99, 0.8, station_id="left"),
        _station(51.0, 4.05, 0.9, station_id="right"),
    ]
    storms = [
        {
            "storm_id": "storm-left",
            "footprint": [
                (50.99, 3.98),
                (50.99, 4.00),
                (51.01, 4.00),
                (51.01, 3.98),
            ],
        },
        {
            "storm_id": "storm-right",
            "footprint": [
                (50.99, 4.04),
                (50.99, 4.06),
                (51.01, 4.06),
                (51.01, 4.04),
            ],
        },
    ]

    result = netatmo_local_module.build_local_netatmo_summary(
        observations,
        51.0,
        4.02,
        storms=storms,
        now=1_000.0,
    )

    assert result["ground_precipitation_observed"] is True
    assert result["ground_confirmed"] is False
    assert result["ground_confirmed_storm_id"] is None
    assert result["ground_confirmed_storm_ids"] == []
    assert result["ground_confirmation_by_storm"]["storm-left"][
        "raining_station_count"
    ] == 1
    assert result["ground_confirmation_by_storm"]["storm-right"][
        "raining_station_count"
    ] == 1


def test_overlapping_footprints_do_not_force_ambiguous_system_confirmation(
    netatmo_local_module,
):
    observations = [
        _station(51.0, 4.0, 0.8, station_id="one"),
        _station(51.005, 4.005, 0.9, station_id="two"),
    ]
    shared_footprint = [
        (50.99, 3.99),
        (50.99, 4.02),
        (51.02, 4.02),
        (51.02, 3.99),
    ]

    result = netatmo_local_module.build_local_netatmo_summary(
        observations,
        51.0,
        4.0,
        storms=[
            {"storm_id": "overlap-a", "footprint": shared_footprint},
            {"storm_id": "overlap-b", "footprint": shared_footprint},
        ],
        now=1_000.0,
    )

    assert result["ground_precipitation_observed"] is True
    assert result["ground_confirmed"] is False
    assert result["ground_eligible_station_count"] == 0
    assert all(
        item["storm_match_ambiguous"]
        for item in result["ground_assessments"]
    )


def test_two_wet_stations_for_same_storm_report_confirmed_id(
    netatmo_local_module,
):
    storm = {
        "storm_id": "storm-same",
        "footprint": [
            (50.98, 3.98),
            (50.98, 4.02),
            (51.02, 4.02),
            (51.02, 3.98),
        ],
    }

    result = netatmo_local_module.build_local_netatmo_summary(
        [
            _station(51.0, 3.99, 0.8, station_id="one"),
            _station(51.0, 4.01, 0.9, station_id="two"),
        ],
        51.0,
        4.0,
        storms=[storm],
        footprint_tolerance_km=0.2,
        now=1_000.0,
    )

    assert result["ground_confirmed"] is True
    assert result["ground_confirmed_storm_id"] == "storm-same"
    assert result["ground_confirmed_storm_ids"] == ["storm-same"]
    assert result["ground_confirmation_by_storm"]["storm-same"][
        "confirmed"
    ] is True
    assert result["ground_confirmation_by_storm"]["storm-same"][
        "confirmation_basis"
    ] == "netatmo_grondmeting"
    assert result["ground_confirmation_by_storm"]["storm-same"][
        "confidence_percent"
    ] == result["ground_confidence_percent"]


def test_station_near_middle_of_footprint_edge_matches_system(
    netatmo_local_module,
):
    storm = {
        "storm_id": "edge-system",
        "footprint": [
            (50.8, 4.10),
            (51.2, 4.10),
            (51.2, 4.20),
            (50.8, 4.20),
        ],
    }

    result = netatmo_local_module.build_local_netatmo_summary(
        [_station(51.0, 4.0, 0.8, station_id="edge")],
        51.0,
        4.0,
        storms=[storm],
        footprint_tolerance_km=8.0,
        now=1_000.0,
    )

    assessment = result["ground_assessments"][0]
    assert assessment["storm_id"] == "edge-system"
    assert assessment["eligible"] is True
    assert assessment["distance_storm_km"] == pytest.approx(7.0, abs=0.2)


def test_same_physical_site_from_metar_and_synoptic_counts_once(
    netatmo_local_module,
):
    footprint = [
        (40.6, -74.2), (40.6, -73.7), (40.9, -73.7), (40.9, -74.2),
    ]
    result = netatmo_local_module.build_local_netatmo_summary(
        [
            _station(
                40.6413,
                -73.7781,
                0.6,
                station_id="KJFK",
                source="metar",
            ),
            _station(
                40.6413,
                -73.7781,
                0.7,
                station_id="synoptic:KJFK",
                source="synoptic",
            ),
        ],
        40.6413,
        -73.7781,
        storms=[{"storm_id": "new-york", "footprint": footprint}],
        now=1_000.0,
    )

    assert result["ground_raw_observation_count"] == 2
    assert result["ground_physical_site_count"] == 1
    assert result["ground_duplicate_observation_count"] == 1
    assert result["ground_system_raining_station_count"] == 1
    assert result["ground_confirmed"] is False
    assert sum(
        assessment["counts_as_independent"]
        for assessment in result["ground_assessments"]
    ) == 1


def test_dry_duplicate_provider_is_not_credited_as_raining_source(
    netatmo_local_module,
):
    timestamp = 1_000.0
    observations = [
        _station(
            40.0, -74.0, 0.0, timestamp=timestamp,
            station_id="metar:KJFK", source="metar",
        ),
        _station(
            40.0, -74.0, 0.8, timestamp=timestamp,
            station_id="synoptic:KJFK", source="synoptic",
        ),
        _station(
            40.01, -74.0, 0.9, timestamp=timestamp,
            station_id="synoptic:KNYC", source="synoptic",
        ),
    ]
    storm = {
        "storm_id": "new-york-system",
        "footprint": [
            (39.98, -74.02), (39.98, -73.98),
            (40.03, -73.98), (40.03, -74.02),
        ],
        "footprint_timestamp": timestamp,
    }

    result = netatmo_local_module.build_local_netatmo_summary(
        observations,
        40.005,
        -74.0,
        storms=[storm],
        now=timestamp,
    )

    detail = result["ground_confirmation_by_storm"][
        "new-york-system"
    ]
    assert result["ground_confirmed"] is True
    assert result["ground_sources"] == ["metar", "synoptic"]
    assert result["ground_raining_sources"] == ["synoptic"]
    assert result["ground_confirmation_basis"] == "synoptic_grondmeting"
    assert detail["sources"] == ["synoptic"]
    assert detail["station_ids"] == [
        "synoptic:KJFK", "synoptic:KNYC",
    ]


def test_two_unique_sites_still_confirm_when_one_has_duplicate_network_rows(
    netatmo_local_module,
):
    footprint = [
        (40.6, -74.2), (40.6, -73.7), (40.9, -73.7), (40.9, -74.2),
    ]
    result = netatmo_local_module.build_local_netatmo_summary(
        [
            _station(
                40.6413, -73.7781, 0.6,
                station_id="KJFK", source="metar",
            ),
            _station(
                40.6413, -73.7781, 0.7,
                station_id="synoptic:KJFK", source="synoptic",
            ),
            _station(
                40.75, -73.95, 0.5,
                station_id="KNYC", source="metar",
            ),
        ],
        40.70,
        -73.86,
        local_radius_km=30.0,
        storms=[{"storm_id": "new-york", "footprint": footprint}],
        now=1_000.0,
    )

    assert result["ground_physical_site_count"] == 2
    assert result["ground_confirmed"] is True
    assert result["ground_confirmation_by_storm"]["new-york"][
        "raining_station_count"
    ] == 2


def test_object_storm_uses_footprint_closest_to_observation_time(
    netatmo_local_module,
):
    old_frame = SimpleNamespace(
        timestamp=1_000.0,
        footprint_points=(
            (50.99, 3.99), (50.99, 4.01), (51.01, 4.01), (51.01, 3.99),
        ),
    )
    latest_frame = SimpleNamespace(
        timestamp=1_600.0,
        footprint_points=(
            (51.9, 4.9), (51.9, 5.1), (52.1, 5.1), (52.1, 4.9),
        ),
    )
    storm = SimpleNamespace(
        storm_id="timed-system",
        radar_system_frames={"old": old_frame, "latest": latest_frame},
        hull=[(49.0, 2.0), (49.0, 6.0), (53.0, 6.0), (53.0, 2.0)],
    )

    result = netatmo_local_module.build_local_netatmo_summary(
        [_station(51.0, 4.0, 0.8, timestamp=1_010.0)],
        51.0,
        4.0,
        storms=[storm],
        now=1_020.0,
    )

    assessment = result["ground_assessments"][0]
    assert assessment["storm_id"] == "timed-system"
    assert assessment["distance_storm_km"] == 0.0


def test_object_storm_rejects_footprint_too_far_from_observation_time(
    netatmo_local_module,
):
    storm = SimpleNamespace(
        storm_id="stale-system",
        radar_system_frames={
            "old": SimpleNamespace(
                timestamp=1_000.0,
                footprint_points=(
                    (50.99, 3.99), (50.99, 4.01),
                    (51.01, 4.01), (51.01, 3.99),
                ),
            )
        },
        hull=[(50.5, 3.5), (50.5, 4.5), (51.5, 4.5), (51.5, 3.5)],
    )

    result = netatmo_local_module.build_local_netatmo_summary(
        [_station(51.0, 4.0, 0.8, timestamp=2_000.0)],
        51.0,
        4.0,
        storms=[storm],
        now=2_010.0,
    )

    assert result["ground_eligible_station_count"] == 0
    assert result["ground_confirmed"] is False
    assert result["ground_assessments"][0]["storm_id"] is None


def test_future_ground_observation_keeps_signed_age_and_is_never_current(
    netatmo_local_module,
):
    result = netatmo_local_module.build_local_netatmo_summary(
        [
            _station(
                51.0,
                4.0,
                0.8,
                timestamp=1_100.0,
                station_id="future",
            )
        ],
        51.0,
        4.0,
        now=1_000.0,
    )

    assessment = result["ground_assessments"][0]
    assert assessment["age_seconds"] == -100.0
    assert assessment["fresh"] is False
    assert assessment["observed_at"] == 1_100.0
    assert result["ground_precipitation_observed"] is False
    assert result["ground_stale_station_count"] == 1


def test_distinct_netatmo_macs_with_same_last_octet_do_not_collide(
    netatmo_local_module,
):
    result = netatmo_local_module.build_local_netatmo_summary(
        [
            _station(
                51.0,
                4.0,
                0.8,
                station_id="netatmo:70:ee:50:29:59:56",
            ),
            _station(
                51.01,
                4.0,
                0.6,
                station_id="netatmo:aa:bb:cc:dd:ee:56",
            ),
        ],
        51.0,
        4.0,
        now=1_000.0,
    )

    physical_ids = {
        assessment["physical_site_id"]
        for assessment in result["ground_assessments"]
    }
    assert result["ground_physical_site_count"] == 2
    assert physical_ids == {
        "netatmo:70:EE:50:29:59:56",
        "netatmo:AA:BB:CC:DD:EE:56",
    }
    assert result["ground_precipitation_observed"] is True


def test_proximity_dedup_is_order_and_tracker_invariant_without_alias_claim(
    netatmo_local_module,
):
    observations = [
        _station(
            51.0,
            4.0,
            0.5,
            station_id="puppy-a",
        ),
        _station(
            51.00063,
            4.0,
            0.5,
            station_id="puppy-b",
        ),
        _station(
            51.00126,
            4.0,
            0.5,
            station_id="puppy-c",
        ),
    ]

    first = netatmo_local_module.build_local_netatmo_summary(
        observations,
        51.0,
        4.0,
        now=1_000.0,
    )
    second = netatmo_local_module.build_local_netatmo_summary(
        list(reversed(observations)),
        51.00126,
        4.0,
        now=1_000.0,
    )

    def independent_assessment(result):
        return next(
            assessment
            for assessment in result["ground_assessments"]
            if assessment["counts_as_independent"]
        )

    first_site = independent_assessment(first)
    second_site = independent_assessment(second)
    assert first["ground_physical_site_count"] == 1
    assert second["ground_physical_site_count"] == 1
    assert first_site["physical_site_id"] == second_site["physical_site_id"]
    assert first_site["physical_site_id"].startswith("proximity-group:")
    assert (
        first_site["physical_site_basis"]
        == "proximity_conservative_dedup"
    )


def test_unknown_provider_has_no_freshness_policy_and_cannot_vote_current(
    netatmo_local_module,
):
    result = netatmo_local_module.build_local_netatmo_summary(
        [
            _station(
                51.0,
                4.0,
                0.8,
                station_id="unknown-provider",
                source="mystery",
            )
        ],
        51.0,
        4.0,
        now=1_000.0,
    )

    assessment = result["ground_assessments"][0]
    assert assessment["freshness_policy_known"] is False
    assert assessment["freshness_limit_seconds"] is None
    assert assessment["fresh"] is False
    assert result["ground_precipitation_observed"] is False


def test_ground_assessment_output_is_bounded_without_losing_current_rain(
    netatmo_local_module,
):
    observations = [
        _station(
            -80.0 + index * (160.0 / 4_999.0),
            -170.0,
            0.0,
            station_id=f"remote-{index:04d}",
        )
        for index in range(4_999)
    ]
    observations.append(
        _station(
            51.0,
            4.0,
            0.8,
            station_id="wet-record-at-end",
        )
    )

    result = netatmo_local_module.build_local_netatmo_summary(
        observations,
        51.0,
        4.0,
        now=1_000.0,
    )

    diagnostic = result["ground_assessment_collection"]
    assert diagnostic["input_total_count"] == 5_000
    assert diagnostic["serialized_count"] == 128
    assert diagnostic["overflow_count"] == 4_872
    assert diagnostic["complete"] is False
    assert len(diagnostic["content_sha256"]) == 64
    assert diagnostic["reason_codes"] == [
        "ground_assessment_serialization_bounded"
    ]
    assert len(result["ground_assessments"]) == 128
    assert result["ground_assessments"][0]["station_id"] == (
        "wet-record-at-end"
    )
    assert result["ground_precipitation_observed"] is True
