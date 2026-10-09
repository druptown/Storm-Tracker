from __future__ import annotations

from storm_tracker_worker.thin_station_catalog import (
    DiscoveryCheckpoint,
    GroundStation,
    StationCatalogRepository,
    StationSelectionRequest,
    normalize_capabilities,
    select_stations,
)


NOW = 1_800_000_000.0


def _station(
    provider: str,
    station_id: str,
    latitude: float,
    longitude: float,
    *capabilities: str,
    quality: float = 1.0,
) -> GroundStation:
    return GroundStation.create(
        provider_id=provider,
        station_id=station_id,
        latitude=latitude,
        longitude=longitude,
        capabilities=capabilities,
        discovered_at=NOW - 3600.0,
        last_seen_at=NOW - 60.0,
        quality_score=quality,
    )


def test_station_identity_and_capabilities_are_canonical():
    station = _station(
        " WUNDERGROUND ", " IMECHE17 ", 51.05, 4.42,
        " Rain_Rate ", "TEMPERATURE", "rain_rate",
    )

    assert station.provider_id == "wunderground"
    assert station.station_id == "IMECHE17"
    assert station.station_key == "wunderground:imeche17"
    assert station.capabilities == frozenset({"rain_rate", "temperature"})
    assert normalize_capabilities([" Pressure ", "pressure", "WIND_SPEED"]) == (
        "pressure", "wind_speed",
    )


def test_selection_uses_smallest_capable_set_and_deduplicates_shared_regions():
    stations = (
        _station("netatmo", "near-rain", 51.050, 4.420, "rain_rate"),
        _station("metar", "near-wind", 51.051, 4.421, "wind_speed"),
        _station("netatmo", "far-both", 51.080, 4.480, "rain_rate", "wind_speed"),
        _station("netatmo", "outside", 52.000, 5.000, "rain_rate", "wind_speed"),
    )
    requests = (
        StationSelectionRequest(
            region_id="region:home",
            latitude=51.05,
            longitude=4.42,
            radius_km=20.0,
            required_capabilities=("rain_rate", "wind_speed"),
        ),
        StationSelectionRequest(
            region_id="region:home",
            latitude=51.051,
            longitude=4.421,
            radius_km=20.0,
            required_capabilities=("rain_rate",),
        ),
    )

    plan = select_stations(stations, requests, now=NOW, maximum_per_region=3)

    assert tuple(item.station_id for item in plan.regions["region:home"]) == (
        "far-both",
    )
    assert tuple(item.station_id for item in plan.stations) == ("far-both",)


def test_stale_discovery_checkpoint_resumes_from_saved_cursor():
    fresh = DiscoveryCheckpoint(
        provider_id="netatmo",
        tile_id="n51e004",
        cursor="page-17",
        completed=False,
        station_count=124,
        updated_at=NOW - 60.0,
    )
    old_complete = DiscoveryCheckpoint(
        provider_id="netatmo",
        tile_id="n50e004",
        cursor=None,
        completed=True,
        station_count=92,
        updated_at=NOW - 31 * 86400.0,
    )

    assert fresh.resume_cursor == "page-17"
    assert fresh.needs_refresh(now=NOW, maximum_age_seconds=30 * 86400.0)
    assert old_complete.needs_refresh(now=NOW, maximum_age_seconds=30 * 86400.0)


def test_selection_prefers_directional_coverage_between_equal_size_sets():
    stations = (
        _station("netatmo", "rain-east", 51.05, 4.421, "rain_rate"),
        _station("netatmo", "pressure-east", 51.05, 4.44, "pressure"),
        _station("metar", "pressure-west", 51.05, 4.39, "pressure"),
    )
    request = StationSelectionRequest(
        region_id="region:home",
        latitude=51.05,
        longitude=4.42,
        radius_km=10.0,
        required_capabilities=("rain_rate", "pressure"),
    )

    selected = select_stations(stations, (request,), now=NOW)

    assert tuple(item.station_id for item in selected.stations) == (
        "pressure-west",
        "rain-east",
    )


class _Cursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, parameters=None):
        self.connection.calls.append((" ".join(sql.split()), parameters))
        self.connection.last_parameters = parameters

    def executemany(self, sql, parameters):
        normalized = " ".join(sql.split())
        self.connection.calls.extend((normalized, row) for row in parameters)

    def fetchone(self):
        return self.connection.rows.get(self.connection.last_parameters)

    def fetchall(self):
        return self.connection.fetchall_rows


class _Connection:
    def __init__(self, rows=None, fetchall_rows=()):
        self.calls = []
        self.commits = 0
        self.rows = {} if rows is None else rows
        self.last_parameters = None
        self.fetchall_rows = tuple(fetchall_rows)

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        raise AssertionError("unexpected rollback")

    def close(self):
        return None


def test_repository_upsert_is_idempotent_and_contains_no_secret_fields():
    connection = _Connection()
    repository = StationCatalogRepository(lambda **kwargs: connection, {})
    station = _station(
        "wunderground", "IMECHE17", 51.05, 4.42,
        "rain_rate", "temperature",
    )

    repository.upsert_stations((station, station))

    station_calls = [
        (sql, params) for sql, params in connection.calls
        if sql.startswith("INSERT INTO stv3_ground_station")
    ]
    assert len(station_calls) == 1
    assert "ON CONFLICT(provider_id,station_id) DO UPDATE" in station_calls[0][0]
    assert station_calls[0][1][0:2] == ("wunderground", "IMECHE17")
    serialized = repr(connection.calls).lower()
    assert "password" not in serialized
    assert "api_key" not in serialized
    assert connection.commits == 1


def test_repository_saves_exact_checkpoint_for_restart():
    connection = _Connection()
    repository = StationCatalogRepository(lambda **kwargs: connection, {})
    checkpoint = DiscoveryCheckpoint(
        provider_id="netatmo",
        tile_id="n51e004",
        cursor="page-17",
        completed=False,
        station_count=124,
        updated_at=NOW,
    )

    repository.save_checkpoint(checkpoint)

    sql, parameters = connection.calls[-1]
    assert sql.startswith("INSERT INTO stv3_station_discovery_tile")
    assert "ON CONFLICT(provider_id,tile_id) DO UPDATE" in sql
    assert parameters == (
        "netatmo", "n51e004", "page-17", False, 124, NOW,
    )
    assert connection.commits == 1


def test_repository_loads_checkpoint_cursor_for_restart():
    connection = _Connection({
        ("netatmo", "n51e004"): (
            "netatmo", "n51e004", "page-17", False, 124, NOW,
        ),
    })
    repository = StationCatalogRepository(lambda **kwargs: connection, {})

    checkpoint = repository.load_checkpoint("netatmo", "n51e004")

    assert checkpoint == DiscoveryCheckpoint(
        provider_id="netatmo",
        tile_id="n51e004",
        cursor="page-17",
        completed=False,
        station_count=124,
        updated_at=NOW,
    )


def test_repository_loads_nearby_stations_with_capabilities():
    connection = _Connection(fetchall_rows=((
        "wunderground", "IMECHE17", 51.05, 4.42,
        ["rain_rate", "pressure"], NOW - 1000.0, NOW - 20.0,
        0.95, True,
    ),))
    repository = StationCatalogRepository(lambda **kwargs: connection, {})

    stations = repository.find_nearby(
        latitude=51.05,
        longitude=4.42,
        radius_km=20.0,
    )

    assert len(stations) == 1
    assert stations[0].station_key == "wunderground:imeche17"
    assert stations[0].capabilities == frozenset({"rain_rate", "pressure"})
    sql, parameters = connection.calls[-1]
    assert "ST_DWithin" in sql
    assert parameters == (4.42, 51.05, 20_000.0, 4.42, 51.05)
