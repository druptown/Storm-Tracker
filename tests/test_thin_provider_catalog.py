from __future__ import annotations

from storm_tracker_worker.desired_state import DesiredState, RuntimeSettings, Target
from storm_tracker_worker.thin_provider_catalog import build_due_requests
from storm_tracker_worker.thin_region_planner import ThinRegionPlanner


def _target(
    target_id: str,
    latitude: float,
    longitude: float,
    *,
    country_code: str,
    kind: str = "test",
    available: bool = True,
    position_observed_at: float | None = 590.0,
) -> Target:
    return Target(
        target_id=target_id,
        display_name=target_id.title(),
        kind=kind,
        available=available,
        latitude=latitude,
        longitude=longitude,
        position_observed_at=position_observed_at if available else None,
        location_accuracy_m=10.0,
        observation_radius_km=50.0,
        country_code=country_code,
    )


def _state(*targets: Target) -> DesiredState:
    return DesiredState(
        producer_id="ha-nas",
        authority_epoch=1,
        generation=1,
        issued_at=590.0,
        previous_state_sha256=None,
        targets=targets,
        settings=RuntimeSettings(
            ground_radius_km=50.0,
            lightning_source_mode="direct",
            model_guidance_enabled=True,
            region_sharing_distance_km=100.0,
        ),
        canonical_bytes=b"{}",
        state_sha256="0" * 64,
    )


def test_belgian_target_selects_belgian_operational_sources_only():
    state = _state(_target("home", 50.8503, 4.3517, country_code="BE"))

    requests = build_due_requests(state, now=600.0, last_attempts={})
    provider_ids = {item.provider_id for item in requests}

    assert provider_ids >= {
        "kmi_aws",
        "kmi_appv4",
        "kmi_radqpe",
        "opera_radar",
        "open_meteo",
        "metar",
        "wunderground",
    }
    assert "aemet_radar" not in provider_ids


def test_provider_boundary_excludes_target_outside_coverage():
    state = _state(_target("madrid", 40.4168, -3.7038, country_code="ES"))

    provider_ids = {
        item.provider_id
        for item in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert "kmi_appv4" not in provider_ids
    assert "kmi_aws" not in provider_ids
    assert "kmi_radqpe" not in provider_ids
    assert "knmi_radar" not in provider_ids
    assert "aemet_radar" in provider_ids
    assert "meteofrance_radar" not in provider_ids


def test_french_target_selects_meteofrance_radar():
    state = _state(_target("paris", 48.8566, 2.3522, country_code="FR"))

    provider_ids = {
        item.provider_id
        for item in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert "meteofrance_radar" in provider_ids


def test_satellite_precipitation_catalogue_respects_product_coverage():
    belgium = _state(_target("home", 51.05, 4.42, country_code="BE"))
    california = _state(_target("sf", 37.7749, -122.4194, country_code="US"))

    belgian_ids = {
        item.provider_id
        for item in build_due_requests(belgium, now=600.0, last_attempts={})
    }
    california_ids = {
        item.provider_id
        for item in build_due_requests(california, now=600.0, last_attempts={})
    }

    assert {"goes19_rrqpe", "noaa_err", "hsaf_h40b"} <= belgian_ids
    assert "goes18_rrqpe" not in belgian_ids
    assert {"goes18_rrqpe", "goes19_rrqpe", "noaa_err"} <= california_ids
    assert "hsaf_h40b" not in california_ids


def test_goes18_catalogue_coverage_wraps_across_the_dateline():
    fiji = _state(_target("fiji", -17.7134, 178.0650, country_code="FJ"))

    provider_ids = {
        item.provider_id
        for item in build_due_requests(fiji, now=600.0, last_attempts={})
    }

    assert "goes18_glm" in provider_ids
    assert "goes18_rrqpe" in provider_ids


def test_italian_target_selects_dpc_sri_radar():
    state = _state(_target("rome", 41.9028, 12.4964, country_code="IT"))

    provider_ids = {
        request.provider_id
        for request in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert "dpc_radar" in provider_ids
    assert "italiameteo_icon2i" in provider_ids
    assert "italiameteo_dpc_catalog" in provider_ids


def test_belgian_target_does_not_select_italiameteo_historical_catalog():
    state = _state(_target("home", 51.05, 4.42, country_code="BE"))

    provider_ids = {
        request.provider_id
        for request in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert "italiameteo_dpc_catalog" not in provider_ids


def test_austrian_target_selects_geosphere_nowcast_only_inside_coverage():
    vienna = _state(_target("vienna", 48.2082, 16.3738, country_code="AT"))
    brussels = _state(_target("brussels", 50.8503, 4.3517, country_code="BE"))

    vienna_ids = {
        item.provider_id
        for item in build_due_requests(vienna, now=600.0, last_attempts={})
    }
    brussels_ids = {
        item.provider_id
        for item in build_due_requests(brussels, now=600.0, last_attempts={})
    }

    assert "geosphere" in vienna_ids
    assert "geosphere" not in brussels_ids


def test_luxembourg_target_selects_meteolux_validation_only_inside_coverage():
    luxembourg = _state(
        _target("luxembourg", 49.6116, 6.1319, country_code="LU"),
    )
    brussels = _state(_target("brussels", 50.8503, 4.3517, country_code="BE"))

    luxembourg_ids = {
        item.provider_id
        for item in build_due_requests(luxembourg, now=600.0, last_attempts={})
    }
    brussels_ids = {
        item.provider_id
        for item in build_due_requests(brussels, now=600.0, last_attempts={})
    }

    assert "meteolux" in luxembourg_ids
    assert "meteolux" not in brussels_ids


def test_uk_target_selects_met_office_rate_radar():
    state = _state(_target("london", 51.5074, -0.1278, country_code="GB"))

    provider_ids = {
        request.provider_id
        for request in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert "metoffice_radar" in provider_ids


def test_global_sources_apply_to_north_american_target():
    state = _state(_target("new_york", 40.7128, -74.0060, country_code="US"))

    provider_ids = {
        item.provider_id
        for item in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert provider_ids >= {"open_meteo", "metar", "blitzortung", "rainviewer"}
    assert {"goes18_glm", "goes19_glm"} <= provider_ids
    assert "eumetsat_li" not in provider_ids
    assert "opera_radar" not in provider_ids


def test_lightning_catalogue_selects_eumetsat_for_belgium():
    state = _state(_target("home", 51.05, 4.42, country_code="BE"))

    provider_ids = {
        item.provider_id
        for item in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert "eumetsat_li" in provider_ids
    assert "goes18_glm" not in provider_ids


def test_global_sources_apply_to_african_target():
    state = _state(_target("nairobi", -1.2864, 36.8172, country_code="KE"))

    provider_ids = {
        item.provider_id
        for item in build_due_requests(state, now=600.0, last_attempts={})
    }

    assert provider_ids >= {"open_meteo", "metar", "blitzortung", "rainviewer"}
    assert "opera_radar" not in provider_ids


def test_one_provider_request_groups_multiple_targets_in_its_coverage():
    state = _state(
        _target("home", 50.8503, 4.3517, country_code="BE"),
        _target("work", 51.2194, 4.4025, country_code="BE"),
    )

    requests = build_due_requests(state, now=600.0, last_attempts={})
    opera_requests = [item for item in requests if item.provider_id == "opera_radar"]

    assert len(opera_requests) == 1
    assert tuple(target.target_id for target in opera_requests[0].targets) == (
        "home",
        "work",
    )


def test_provider_is_not_due_again_before_its_cadence():
    state = _state(_target("home", 50.8503, 4.3517, country_code="BE"))

    requests = build_due_requests(
        state,
        now=600.0,
        last_attempts={"open_meteo": 550.0},
    )

    assert "open_meteo" not in {item.provider_id for item in requests}


def test_unavailable_target_creates_no_requests():
    state = _state(
        _target(
            "offline",
            50.8503,
            4.3517,
            country_code="BE",
            available=False,
        )
    )

    assert build_due_requests(state, now=600.0, last_attempts={}) == ()


def test_available_stationary_target_remains_usable_when_position_is_old():
    state = _state(
        _target(
            "home",
            50.8503,
            4.3517,
            country_code="BE",
            position_observed_at=1.0,
        )
    )

    provider_ids = {
        item.provider_id
        for item in build_due_requests(state, now=86_400.0, last_attempts={})
    }

    assert provider_ids >= {"opera_radar", "open_meteo", "metar"}


def test_available_static_target_without_position_timestamp_remains_usable():
    state = _state(
        _target(
            "fixed_test_tracker",
            50.8503,
            4.3517,
            country_code="BE",
            position_observed_at=None,
        )
    )

    assert build_due_requests(state, now=86_400.0, last_attempts={})


def test_due_requests_carry_shared_region_plan_instead_of_replanning_targets():
    state = _state(
        _target("home", 51.05, 4.42, country_code="BE", kind="home"),
        _target("nearby", 51.10, 4.42, country_code="BE"),
    )
    plan = ThinRegionPlanner().plan(state, now=600.0)

    requests = build_due_requests(
        state,
        now=600.0,
        last_attempts={},
        region_plan=plan,
    )
    netatmo = next(item for item in requests if item.provider_id == "netatmo")

    assert tuple(target.target_id for target in netatmo.targets) == ("home", "nearby")
    assert len(netatmo.regions) == 1
    assert netatmo.regions[0].region_id == "region:home"
    assert netatmo.ground_radius_km == 50.0


def test_region_grace_keeps_unavailable_person_in_provider_requests():
    planner = ThinRegionPlanner(unavailable_grace_seconds=900.0)
    available = _state(
        _target("person", 51.05, 4.42, country_code="BE")
    )
    planner.plan(available, now=100.0)
    unavailable = _state(
        _target(
            "person",
            0.0,
            0.0,
            country_code="BE",
            available=False,
        )
    )
    plan = planner.plan(unavailable, now=200.0)

    requests = build_due_requests(
        unavailable,
        now=200.0,
        last_attempts={},
        region_plan=plan,
    )

    assert requests
    assert all(request.targets[0].latitude == 51.05 for request in requests)
