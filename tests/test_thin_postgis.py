from __future__ import annotations

import json

import pytest

from storm_tracker_worker.thin_fusion import fuse_observations
from storm_tracker_worker.thin_ground_validation import ForecastValidation
from storm_tracker_worker.thin_nowcast import StormCell, ThinNowcastRuntime
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_postgis import ThinPostGISSettings, ThinPostGISWriter
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_regional_radar_fusion import RegionalFusionSummary
from storm_tracker_worker.desired_state import Target
from storm_tracker_worker.thin_provider_catalog import ThinProviderRequest
from storm_tracker_worker.thin_satellite_adapters import GoesRrqpeAdapter


def _cycle_and_nowcast(valid_at: float, *, observation_count: int = 1):
    observations = tuple(
        NormalizedObservation.create(
            provider_id="opera_radar",
            product_id="composite",
            target_id="home",
            kind=ObservationKind.RAIN_RATE,
            value=2.5 + index,
            unit="mm/h",
            observed_at=valid_at,
            received_at=valid_at + 1.0,
            latitude=50.85,
            longitude=4.35 + index / 1000.0,
            quality=ObservationQuality.GOOD,
            geometry={
                "type": "Point",
                "coordinates": [4.35 + index / 1000.0, 50.85],
            },
            metadata={"cell_id": f"cell-{valid_at}-{index}"},
        )
        for index in range(observation_count)
    )
    provider = ProviderResult(
        provider_id="opera_radar",
        ready=True,
        status="operational",
        reason_code=None,
        observations=observations,
        attempted_at=valid_at,
        completed_at=valid_at + 1.0,
    )
    cycle = fuse_observations(
        {"opera_radar": provider},
        evaluated_at=valid_at + 2.0,
        target_ids=("home",),
    )
    nowcast = ThinNowcastRuntime(targets={"home": (50.85, 4.35)}).advance(cycle)
    return cycle, nowcast


def _station_cycle_and_nowcast(
    valid_at: float,
    *target_ids: str,
):
    observations = tuple(
        NormalizedObservation.create(
            provider_id="netatmo",
            product_id="getpublicdata",
            target_id=target_id,
            kind=ObservationKind.RAIN_RATE,
            value=2.5,
            unit="mm/h",
            observed_at=valid_at,
            received_at=valid_at + 1.0,
            latitude=51.05,
            longitude=4.42,
            quality=ObservationQuality.GOOD,
            metadata={
                "station_id": "station-1",
                "measurement": "rain_rate",
            },
        )
        for target_id in target_ids
    )
    provider = ProviderResult(
        provider_id="netatmo",
        ready=True,
        status="operational",
        reason_code=None,
        observations=observations,
        attempted_at=valid_at,
        completed_at=valid_at + 1.0,
    )
    cycle = fuse_observations(
        {"netatmo": provider},
        evaluated_at=valid_at + 2.0,
        target_ids=target_ids,
    )
    targets = {
        target_id: (51.05, 4.42)
        for target_id in target_ids
    }
    return cycle, ThinNowcastRuntime(targets=targets).advance(cycle)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, parameters=None):
        self.connection.statements.append((" ".join(sql.split()), parameters))

    def executemany(self, sql, parameters):
        normalized = " ".join(sql.split())
        rows = tuple(parameters)
        self.connection.batch_calls.append((normalized, rows))
        self.connection.statements.extend((normalized, row) for row in rows)


class FakeConnection:
    def __init__(self, statements, batch_calls=None):
        self.statements = statements
        self.batch_calls = [] if batch_calls is None else batch_calls
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class ConnectionFactory:
    def __init__(self):
        self.statements = []
        self.batch_calls = []
        self.connections = []

    def __call__(self, **kwargs):
        connection = FakeConnection(self.statements, self.batch_calls)
        self.connections.append(connection)
        return connection


def _settings():
    return ThinPostGISSettings(
        host="192.0.2.10",
        port=5433,
        database="storm_tracker",
        user="storm",
        password="not-printed",
    )


def _regional_summary() -> RegionalFusionSummary:
    return RegionalFusionSummary(
        region_id="region:home",
        grid_id="grid:abc",
        status="current",
        mode="fused",
        evaluated_at=102.0,
        field_observed_at=100.0,
        contributors=("dwd_radolan", "opera_radar"),
        excluded={},
        standby={"rainviewer": "lower_ranked_standby"},
        reason_codes=(),
        coverage_fraction=0.95,
        mean_confidence=0.8,
        disagreement_fraction=0.1,
    )


@pytest.mark.asyncio
async def test_writer_persists_compact_regional_radar_summary_without_raster():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _cycle_and_nowcast(100.0)

    await writer.write_cycle(
        cycle,
        nowcast,
        regional_radar=(_regional_summary(),),
    )

    fusion_row = next(
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_fusion_cycle")
    )
    payload = json.loads(fusion_row[-1])
    assert payload[0]["region_id"] == "region:home"
    assert "rain_rate_mm_h" not in json.dumps(payload)
    assert "valid_mask" not in json.dumps(payload)


@pytest.mark.asyncio
async def test_writer_batches_many_observations_in_one_database_roundtrip():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _cycle_and_nowcast(100.0, observation_count=250)

    result = await writer.write_cycle(cycle, nowcast)

    assert result.status == "current"
    observation_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_observation")
    ]
    assert len(observation_batches) == 1
    assert len(observation_batches[0]) == 250


@pytest.mark.asyncio
async def test_writer_stores_one_physical_station_measurement_for_two_targets():
    cycle, nowcast = _station_cycle_and_nowcast(100.0, "home", "child")
    factory = ConnectionFactory()

    result = await ThinPostGISWriter(
        _settings(),
        connect_factory=factory,
    ).write_cycle(cycle, nowcast)

    batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_station_measurement")
    ]
    assert len(batches) == 1
    assert len(batches[0]) == 1
    assert "home" not in batches[0][0]
    assert "child" not in batches[0][0]
    assert result.written_rows["station_measurements"] == 1
    station_sql = next(
        sql for sql, _ in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_station_measurement")
    )
    assert "EXCLUDED.received_at >= stv3_station_measurement.received_at" in station_sql


@pytest.mark.asyncio
async def test_writer_keeps_nonstation_radar_on_generic_observation_route():
    cycle, nowcast = _cycle_and_nowcast(100.0)
    factory = ConnectionFactory()

    await ThinPostGISWriter(
        _settings(),
        connect_factory=factory,
    ).write_cycle(cycle, nowcast)

    assert any(
        sql.startswith("INSERT INTO stv3_observation")
        for sql, _ in factory.statements
    )


@pytest.mark.asyncio
async def test_station_fk_failure_rolls_back_and_retry_deduplicates_cycle():
    factory = ConnectionFactory()
    original_call = factory.__call__
    calls = {"count": 0}

    def fail_first_station_batch(**kwargs):
        connection = original_call(**kwargs)
        calls["count"] += 1
        if calls["count"] == 1:
            original_cursor = connection.cursor

            def cursor():
                result = original_cursor()
                original_executemany = result.executemany

                def executemany(sql, parameters):
                    if "INSERT INTO stv3_station_measurement" in sql:
                        raise RuntimeError("foreign key violation")
                    return original_executemany(sql, parameters)

                result.executemany = executemany
                return result

            connection.cursor = cursor
        return connection

    writer = ThinPostGISWriter(
        _settings(),
        connect_factory=fail_first_station_batch,
    )
    cycle, nowcast = _station_cycle_and_nowcast(100.0, "home", "child")

    degraded = await writer.write_cycle(cycle, nowcast)
    recovered = await writer.write_cycle(cycle, nowcast)

    assert degraded.status == "degraded"
    assert degraded.queued_cycles == 1
    assert factory.connections[0].rolled_back is True
    assert recovered.status == "current"
    assert recovered.queued_cycles == 0
    successful_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_station_measurement")
    ]
    assert len(successful_batches) == 1
    assert len(successful_batches[0]) == 1


@pytest.mark.asyncio
async def test_writer_persists_quantitative_satellite_observation():
    now = 1_800_000_000.0
    target = Target(
        target_id="home", display_name="Home", kind="test", available=True,
        latitude=40.0, longitude=-75.0, position_observed_at=now,
        location_accuracy_m=10.0, observation_radius_km=100.0,
        country_code="US",
    )
    observations = await GoesRrqpeAdapter(19, fetch_job=lambda job: {
        "frame_timestamp": now - 60.0,
        "rain_samples": [{
            "lat": 40.0, "lon": -75.0, "timestamp": now - 60.0,
            "rain_rate_mm_h": 4.4, "quality": 0.9,
            "source": "noaa_goes19_rrqpe",
        }],
    }).fetch(
        ThinProviderRequest("goes19_rrqpe", "rrqpe", (target,), now),
        None,
        now=now,
    )
    provider = ProviderResult(
        provider_id="goes19_rrqpe", ready=True, status="operational",
        reason_code=None, observations=observations,
        attempted_at=now - 1.0, completed_at=now,
    )
    cycle = fuse_observations(
        {"goes19_rrqpe": provider}, evaluated_at=now, target_ids=("home",),
    )
    nowcast = ThinNowcastRuntime(targets={"home": (40.0, -75.0)}).advance(cycle)
    factory = ConnectionFactory()

    result = await ThinPostGISWriter(
        _settings(), connect_factory=factory,
    ).write_cycle(cycle, nowcast)

    observation_rows = [
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_observation")
    ]
    assert result.status == "current"
    assert len(observation_rows) == 1
    assert observation_rows[0][1] == "goes19_rrqpe"
    assert observation_rows[0][4] == "rain_rate"
    assert observation_rows[0][5] == pytest.approx(4.4)


@pytest.mark.asyncio
async def test_writer_uses_idempotent_upserts_and_geometry_parameters():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _cycle_and_nowcast(100.0)

    first = await writer.write_cycle(cycle, nowcast)
    second = await writer.write_cycle(cycle, nowcast)

    assert first.status == second.status == "current"
    observation_inserts = [
        item for item in factory.statements
        if item[0].startswith("INSERT INTO stv3_observation")
    ]
    assert len(observation_inserts) == 2
    assert all("ON CONFLICT" in sql and "DO UPDATE" in sql for sql, _ in observation_inserts)
    assert observation_inserts[0][1][0] == observation_inserts[1][1][0]
    geometry = json.loads(observation_inserts[0][1][-1])
    assert geometry == {"type": "Point", "coordinates": [4.35, 50.85]}
    assert all(connection.committed for connection in factory.connections)


@pytest.mark.asyncio
async def test_runtime_writer_never_attempts_database_ddl():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _cycle_and_nowcast(100.0)

    result = await writer.write_cycle(cycle, nowcast)

    assert result.status == "current"
    assert not [
        sql
        for sql, _ in factory.statements
        if sql.startswith(("CREATE ", "ALTER ", "GRANT ", "REVOKE ", "DROP "))
    ]


@pytest.mark.asyncio
async def test_writer_persists_polygon_entry_and_exit_passage():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, _ = _cycle_and_nowcast(100.0)
    cell = StormCell(
        cell_id="polygon-cell", provider_id="opera_radar", observed_at=100.0,
        latitude=0.0, longitude=0.0,
        geometry={"type": "Polygon", "coordinates": [[
            [-0.01, -0.01], [0.01, -0.01], [0.01, 0.01],
            [-0.01, 0.01], [-0.01, -0.01],
        ]]},
        precipitation_rate_mm_h=4.0, reflectivity_dbz=None, radar_level=None,
        velocity_east_kmh=60.0, velocity_north_kmh=0.0,
        speed_kmh=60.0, heading_deg=90.0,
    )
    nowcast = ThinNowcastRuntime(targets={"home": (0.0, 0.10)}).advance(
        cycle, tracked_cells=(cell,),
    )

    result = await writer.write_cycle(cycle, nowcast)

    inserts = [
        item for item in factory.statements
        if item[0].startswith("INSERT INTO stv3_target_passage")
    ]
    assert result.written_rows["target_passages"] == 1
    assert len(inserts) == 1
    assert inserts[0][1][4] < inserts[0][1][5]
    assert inserts[0][1][6] == "translated_footprint"


@pytest.mark.asyncio
async def test_writer_persists_validation_idempotently():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _cycle_and_nowcast(100.0)
    validation = ForecastValidation(
        validation_key="v1", target_id="home", evaluated_at=102.0,
        cell_id="cell-1", provider_id="opera_radar",
        station_provider_id="netatmo", station_id="station-1",
        outcome="hit", entry_error_seconds=30.0, exit_error_seconds=None,
        predicted_intensity_mm_h=2.0, observed_intensity_mm_h=2.5,
        intensity_error_mm_h=0.5,
        contributing_stations=("netatmo:station-1",),
    )

    result = await writer.write_cycle(cycle, nowcast, (validation,))

    inserts = [
        item for item in factory.statements
        if item[0].startswith("INSERT INTO stv3_forecast_validation")
    ]
    assert result.written_rows["forecast_validations"] == 1
    assert len(inserts) == 1
    assert "ON CONFLICT(validation_key) DO UPDATE" in inserts[0][0]


@pytest.mark.asyncio
async def test_outage_is_degraded_and_next_call_reconnects_and_drains_queue():
    statements = []
    calls = {"count": 0}

    def flaky_factory(**kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("database offline")
        return FakeConnection(statements)

    writer = ThinPostGISWriter(_settings(), connect_factory=flaky_factory)
    first_cycle, first_nowcast = _cycle_and_nowcast(100.0)
    second_cycle, second_nowcast = _cycle_and_nowcast(200.0)

    degraded = await writer.write_cycle(first_cycle, first_nowcast)
    recovered = await writer.write_cycle(second_cycle, second_nowcast)

    assert degraded.status == "degraded"
    assert degraded.last_error_code == "postgis_unavailable"
    assert degraded.queued_cycles == 1
    assert recovered.status == "current"
    assert recovered.queued_cycles == 0
    assert len([
        sql for sql, _ in statements if sql.startswith("INSERT INTO stv3_fusion_cycle")
    ]) == 2


@pytest.mark.asyncio
async def test_retry_queue_keeps_only_eight_newest_cycles():
    def unavailable(**kwargs):
        raise OSError("database offline")

    writer = ThinPostGISWriter(_settings(), connect_factory=unavailable)
    result = None
    for index in range(10):
        cycle, nowcast = _cycle_and_nowcast(100.0 + index * 10.0)
        result = await writer.write_cycle(cycle, nowcast)

    assert result is not None
    assert result.status == "degraded"
    assert result.queued_cycles == 8
    assert result.dropped_cycles == 2
    assert writer.diagnostics()["last_error_code"] == "postgis_unavailable"
