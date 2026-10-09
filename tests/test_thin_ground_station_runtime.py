from __future__ import annotations

from dataclasses import replace

from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_ground_station_runtime import GroundStationRuntime
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_region_planner import ThinAcquisitionRegion
from storm_tracker_worker.thin_station_catalog import GroundStation


NOW = 1_800_000_000.0


def _target(target_id: str, latitude: float, longitude: float) -> Target:
    return Target(
        target_id=target_id,
        display_name=target_id,
        kind="test",
        available=True,
        latitude=latitude,
        longitude=longitude,
        position_observed_at=NOW,
        location_accuracy_m=5.0,
        observation_radius_km=50.0,
        country_code="BE",
    )


def _station(
    provider_id: str,
    station_id: str,
    latitude: float,
    longitude: float,
    capabilities: tuple[str, ...],
    *,
    age_seconds: float = 0.0,
    quality_score: float = 1.0,
) -> GroundStation:
    return GroundStation.create(
        provider_id=provider_id,
        station_id=station_id,
        latitude=latitude,
        longitude=longitude,
        capabilities=capabilities,
        discovered_at=NOW - 1000.0,
        last_seen_at=NOW - age_seconds,
        quality_score=quality_score,
    )


class Catalog:
    def __init__(self, stations):
        self.stations = tuple(stations)
        self.calls = []

    def find_nearby(self, *, latitude, longitude, radius_km):
        self.calls.append((latitude, longitude, radius_km))
        return self.stations


def _ground_requests(home: Target, nearby: Target):
    region = ThinAcquisitionRegion(
        region_id="region:home",
        kind="home",
        center_latitude=home.latitude,
        center_longitude=home.longitude,
        observation_radius_km=50.0,
        targets=(home, nearby),
        stale_target_ids=(),
    )
    return tuple(
        ThinProviderRequest(
            provider_id=provider_id,
            product_id="ground_observation",
            targets=(home, nearby),
            requested_at=NOW,
            regions=(region,),
            ground_radius_km=20.0,
        )
        for provider_id in ("metar", "netatmo", "wunderground")
    )


def test_planner_selects_nearby_stations_for_every_enabled_provider():
    home = _target("home", 51.05, 4.42)
    nearby = _target("nearby", 51.08, 4.42)
    catalog = Catalog((
        _station("netatmo", "rain-only", 51.051, 4.421, ("rain_rate",)),
        _station(
            "wunderground",
            "all-in-one",
            51.06,
            4.42,
            ("rain_rate", "pressure", "wind_speed", "wind_direction", "temperature"),
        ),
        _station("metar", "EBBR", 50.90, 4.48, ("pressure", "temperature")),
    ))

    planned = GroundStationRuntime(catalog).prepare(_ground_requests(home, nearby), now=NOW)

    by_provider = {request.provider_id: request for request in planned}
    assert by_provider["wunderground"].selected_station_ids == ("all-in-one",)
    assert by_provider["metar"].selected_station_ids == ("EBBR",)
    assert by_provider["netatmo"].selected_station_ids == ("rain-only",)
    assert all(request.station_selection_applied for request in planned)
    assert len(catalog.calls) == 2


def test_planner_selects_nearest_measurement_stations_per_target_then_deduplicates():
    west = _target("west", 51.05, 4.20)
    east = _target("east", 51.05, 4.60)
    catalog = Catalog((
        _station("netatmo", "west-rain", 51.05, 4.201, ("rain_rate",)),
        _station("netatmo", "east-rain", 51.05, 4.599, ("rain_rate",)),
        _station(
            "netatmo",
            "shared-weather",
            51.05,
            4.40,
            ("pressure", "wind_speed", "wind_direction"),
        ),
    ))
    region = ThinAcquisitionRegion(
        region_id="region:shared",
        kind="external",
        center_latitude=51.05,
        center_longitude=4.40,
        observation_radius_km=30.0,
        targets=(west, east),
        stale_target_ids=(),
    )
    request = ThinProviderRequest(
        provider_id="netatmo",
        product_id="ground_observation",
        targets=(west, east),
        requested_at=NOW,
        regions=(region,),
        ground_radius_km=20.0,
    )

    planned = GroundStationRuntime(catalog).prepare((request,), now=NOW)

    assert planned[0].selected_station_ids == (
        "east-rain",
        "shared-weather",
        "west-rain",
    )
    assert len(catalog.calls) == 2


def test_planner_marks_only_targets_without_catalog_match_for_discovery():
    home = _target("home", 51.05, 4.42)
    london = _target("fictive", 51.5074, -0.1278)
    catalog = Catalog((
        _station(
            "netatmo",
            "home-weather",
            51.051,
            4.421,
            ("rain_rate", "pressure", "wind_speed", "wind_direction"),
        ),
    ))
    request = ThinProviderRequest(
        provider_id="netatmo",
        product_id="ground_observation",
        targets=(home, london),
        requested_at=NOW,
        ground_radius_km=20.0,
    )

    planned = GroundStationRuntime(catalog).prepare((request,), now=NOW)

    assert planned[0].selected_station_ids == ("home-weather",)
    assert planned[0].station_discovery_target_ids == ("fictive",)
    assert planned[0].station_selection_reason is None


def test_planner_keeps_three_nearest_rain_stations_for_local_redundancy():
    home = _target("home", 51.05, 4.42)
    catalog = Catalog(tuple(
        _station("netatmo", f"rain-{index}", 51.05, 4.42 + index * 0.01, ("rain_rate",))
        for index in range(1, 5)
    ))
    request = ThinProviderRequest(
        provider_id="netatmo",
        product_id="ground_observation",
        targets=(home,),
        requested_at=NOW,
        ground_radius_km=20.0,
    )

    planned = GroundStationRuntime(catalog).prepare((request,), now=NOW)

    assert planned[0].selected_station_ids == ("rain-1", "rain-2", "rain-3")


def test_planner_deduplicates_shared_region_and_rejects_stale_inventory():
    home = _target("home", 51.05, 4.42)
    nearby = _target("nearby", 51.08, 4.42)
    catalog = Catalog((
        _station(
            "wunderground",
            "stale",
            51.05,
            4.42,
            ("rain_rate", "pressure", "wind_speed", "temperature"),
            age_seconds=8 * 86400.0,
        ),
        _station(
            "netatmo",
            "fresh",
            51.052,
            4.421,
            ("rain_rate", "pressure", "wind_speed", "wind_direction", "temperature"),
        ),
    ))

    planned = GroundStationRuntime(
        catalog,
        maximum_station_age_seconds=7 * 86400.0,
    ).prepare(_ground_requests(home, nearby), now=NOW)

    netatmo = next(item for item in planned if item.provider_id == "netatmo")
    assert netatmo.selected_station_ids == ("fresh",)
    assert "stale" not in {
        station_id for request in planned for station_id in request.selected_station_ids
    }
    assert len(catalog.calls) == 2


def test_offline_catalog_stations_do_not_expire_by_default():
    home = _target("home", 51.05, 4.42)
    nearby = _target("nearby", 51.08, 4.42)
    catalog = Catalog((
        _station(
            "wunderground",
            "stable-metadata",
            51.05,
            4.42,
            ("rain_rate", "pressure", "wind_speed", "wind_direction", "temperature"),
            age_seconds=365 * 86400.0,
        ),
    ))

    planned = GroundStationRuntime(catalog).prepare(
        _ground_requests(home, nearby),
        now=NOW,
    )

    wunderground = next(
        item for item in planned if item.provider_id == "wunderground"
    )
    assert wunderground.selected_station_ids == ("stable-metadata",)


def test_non_ground_requests_are_unchanged():
    home = _target("home", 51.05, 4.42)
    request = ThinProviderRequest("opera_radar", "radar", (home,), NOW)

    planned = GroundStationRuntime(Catalog(())).prepare((request,), now=NOW)

    assert planned == (request,)


def test_temperature_only_catalog_station_is_not_selected_for_live_polling():
    home = _target("home", 51.05, 4.42)
    nearby = _target("nearby", 51.08, 4.42)
    catalog = Catalog((
        _station(
            "netatmo",
            "temperature-only",
            51.051,
            4.421,
            ("temperature", "relative_humidity"),
        ),
        _station("netatmo", "pressure", 51.052, 4.422, ("pressure",)),
    ))

    planned = GroundStationRuntime(catalog).prepare(
        _ground_requests(home, nearby),
        now=NOW,
    )

    netatmo = next(item for item in planned if item.provider_id == "netatmo")
    assert netatmo.selected_station_ids == ("pressure",)
