from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType

import pytest

from storm_tracker_worker.thin_fusion import fuse_observations
from storm_tracker_worker.thin_ground_validation import ForecastValidation
from storm_tracker_worker.thin_footprint_forecast import TargetPassage
from storm_tracker_worker.thin_nowcast import StormCell, ThinNowcast, ThinNowcastRuntime
from storm_tracker_worker.thin_observation import (
    NormalizedObservation,
    ObservationKind,
    ObservationQuality,
)
from storm_tracker_worker.thin_postgis import (
    MAX_FORECAST_METADATA_BYTES,
    ThinPostGISSettings,
    ThinPostGISWriter,
    _bounded_json,
    _forecast_issue_id,
    _passage_id,
)
from storm_tracker_worker.thin_provider_runtime import ProviderResult
from storm_tracker_worker.thin_regional_radar_fusion import RegionalFusionSummary
from storm_tracker_worker.thin_region_planner import (
    ThinAcquisitionRegion,
    ThinRegionPlan,
)
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


def test_forecast_and_passage_ids_preserve_versions_and_reentry() -> None:
    common = dict(
        target_id="home",
        issued_at=100.0,
        policy_version="candidate_policy_v1",
        region_revision_id="region:v1",
        input_fingerprints=("field-a",),
    )
    first_issue = _forecast_issue_id(
        algorithm_version="scientific-passage-v1", **common,
    )
    second_issue = _forecast_issue_id(
        algorithm_version="scientific-passage-v2", **common,
    )

    assert first_issue != second_issue
    assert _passage_id(first_issue, "system:a", 0) != _passage_id(
        first_issue, "system:a", 1,
    )
    assert _passage_id(first_issue, "system:a", 0) == _passage_id(
        first_issue, "system:a", 0,
    )


def test_forecast_metadata_has_a_hard_serialized_size_bound() -> None:
    assert len(_bounded_json({"value": "a" * 100})) < MAX_FORECAST_METADATA_BYTES
    with pytest.raises(ValueError, match="forecast_metadata_too_large"):
        _bounded_json({"value": "a" * MAX_FORECAST_METADATA_BYTES})


def test_scientific_passage_migration_is_additive_and_idempotent() -> None:
    migration = Path(
        "storm_tracker_worker/postgis_migrations/"
        "0008_scientific_passage_forecast.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS public.stv3_target_forecast_issue" in migration
    assert "CREATE TABLE IF NOT EXISTS public.stv3_target_passage_issue" in migration
    assert "ON CONFLICT(migration_id) DO NOTHING" in migration
    assert "DROP TABLE" not in migration.upper()


def _station_cycle_and_nowcast(
    valid_at: float,
    *target_ids: str,
    provider_id: str = "netatmo",
):
    observations = tuple(
        NormalizedObservation.create(
            provider_id=provider_id,
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
        provider_id=provider_id,
        ready=True,
        status="operational",
        reason_code=None,
        observations=observations,
        attempted_at=valid_at,
        completed_at=valid_at + 1.0,
    )
    cycle = fuse_observations(
        {provider_id: provider},
        evaluated_at=valid_at + 2.0,
        target_ids=target_ids,
    )
    targets = {
        target_id: (51.05, 4.42)
        for target_id in target_ids
    }
    return cycle, ThinNowcastRuntime(targets=targets).advance(cycle)


def _shared_source_cycle_and_nowcast(valid_at: float):
    observations = tuple(
        NormalizedObservation.create(
            provider_id="opera_radar",
            product_id="composite",
            target_id=target_id,
            kind=ObservationKind.RAIN_RATE,
            value=4.0,
            unit="mm/h",
            observed_at=valid_at,
            received_at=valid_at + 1.0,
            latitude=38.35,
            longitude=15.8,
            quality=ObservationQuality.GOOD,
            geometry={"type": "Point", "coordinates": [15.8, 38.35]},
            metadata={"source_pixel": "sri:123:456"},
        )
        for target_id in ("home", "child")
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
        target_ids=("home", "child"),
    )
    nowcast = ThinNowcastRuntime(
        targets={"home": (38.35, 15.8), "child": (38.35, 15.8)},
    ).advance(cycle)
    return cycle, nowcast


def _shared_home_region_plan(valid_at: float) -> ThinRegionPlan:
    targets = tuple(
        Target(
            target_id=target_id,
            display_name=target_id.title(),
            kind="test",
            available=True,
            latitude=38.35,
            longitude=15.8,
            position_observed_at=valid_at,
            location_accuracy_m=10.0,
            observation_radius_km=100.0,
            country_code="IT",
        )
        for target_id in ("home", "child")
    )
    region = ThinAcquisitionRegion(
        region_id="region:home",
        region_instance_id="region-instance:home",
        region_revision_id="region-revision:home:v1",
        activated_at=0.0,
        revision_valid_from=valid_at,
        kind="home",
        center_latitude=38.35,
        center_longitude=15.8,
        observation_radius_km=100.0,
        targets=targets,
        stale_target_ids=(),
    )
    return ThinRegionPlan(
        regions=(region,),
        targets=targets,
        target_region_ids=MappingProxyType({
            "home": "region:home",
            "child": "region:home",
        }),
        location_statuses=MappingProxyType({
            "home": "current",
            "child": "current",
        }),
    )


def _split_region_plan(valid_at: float) -> ThinRegionPlan:
    shared = _shared_home_region_plan(valid_at)
    regions = tuple(
        ThinAcquisitionRegion(
            region_id=f"region:{target.target_id}",
            region_instance_id=f"region-instance:{target.target_id}",
            region_revision_id=f"region-revision:{target.target_id}:v1",
            activated_at=valid_at,
            revision_valid_from=valid_at,
            kind="external",
            center_latitude=target.latitude,
            center_longitude=target.longitude,
            observation_radius_km=100.0,
            targets=(target,),
            stale_target_ids=(),
        )
        for target in shared.targets
    )
    return ThinRegionPlan(
        regions=regions,
        targets=shared.targets,
        target_region_ids=MappingProxyType({
            target.target_id: f"region:{target.target_id}"
            for target in shared.targets
        }),
        location_statuses=shared.location_statuses,
    )


def _single_region_plan(
    valid_at: float,
    positions: dict[str, tuple[float, float]],
) -> ThinRegionPlan:
    targets = tuple(
        Target(
            target_id=target_id,
            display_name=target_id.title(),
            kind="test",
            available=True,
            latitude=latitude,
            longitude=longitude,
            position_observed_at=valid_at,
            location_accuracy_m=10.0,
            observation_radius_km=100.0,
            country_code="IT",
        )
        for target_id, (latitude, longitude) in positions.items()
    )
    region = ThinAcquisitionRegion(
        region_id="region:test",
        region_instance_id="region-instance:test",
        region_revision_id="region-revision:test:v1",
        activated_at=valid_at,
        revision_valid_from=valid_at,
        kind="external",
        center_latitude=sum(item.latitude for item in targets) / len(targets),
        center_longitude=sum(item.longitude for item in targets) / len(targets),
        observation_radius_km=100.0,
        targets=targets,
        stale_target_ids=(),
    )
    return ThinRegionPlan(
        regions=(region,),
        targets=targets,
        target_region_ids=MappingProxyType({
            target.target_id: region.region_id for target in targets
        }),
        location_statuses=MappingProxyType({
            target.target_id: "current" for target in targets
        }),
    )
class FakeCursor:
    def __init__(self, connection):
        self.connection = connection
        self.result = ()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, sql, parameters=None):
        normalized = " ".join(sql.split())
        self.connection.statements.append((normalized, parameters))
        if normalized.startswith("SELECT to_regclass('public.stv3_target_forecast_issue')"):
            self.result = ((self.connection.scientific_passage_schema,),)
        elif normalized.startswith("SELECT station.provider_id,station.station_id"):
            pairs = tuple(zip(parameters[0::2], parameters[1::2]))
            self.result = tuple(
                pair for pair in pairs if pair in self.connection.known_stations
            )
        else:
            self.result = ()

    def executemany(self, sql, parameters):
        normalized = " ".join(sql.split())
        rows = tuple(parameters)
        self.connection.batch_calls.append((normalized, rows))
        self.connection.statements.extend((normalized, row) for row in rows)
        if normalized.startswith("INSERT INTO stv3_ground_station"):
            self.connection.known_stations.update(
                (str(row[0]), str(row[1])) for row in rows
            )

    def fetchall(self):
        return self.result

    def fetchone(self):
        return self.result[0] if self.result else None


class FakeConnection:
    def __init__(
        self,
        statements,
        batch_calls=None,
        known_stations=None,
        *,
        scientific_passage_schema=True,
    ):
        self.statements = statements
        self.batch_calls = [] if batch_calls is None else batch_calls
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.known_stations = (
            {("netatmo", "station-1")}
            if known_stations is None else set(known_stations)
        )
        self.scientific_passage_schema = scientific_passage_schema

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


class ConnectionFactory:
    def __init__(self, *, scientific_passage_schema=True):
        self.statements = []
        self.batch_calls = []
        self.connections = []
        self.scientific_passage_schema = scientific_passage_schema

    def __call__(self, **kwargs):
        connection = FakeConnection(
            self.statements,
            self.batch_calls,
            scientific_passage_schema=self.scientific_passage_schema,
        )
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
        if sql.startswith("WITH source AS (")
    ]
    assert len(observation_batches) == 1
    assert len(observation_batches[0]) == 250


@pytest.mark.asyncio
async def test_writer_stores_shared_source_once_and_links_its_region():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _shared_source_cycle_and_nowcast(100.0)

    result = await writer.write_cycle(
        cycle,
        nowcast,
        region_plan=_shared_home_region_plan(100.0),
    )

    observation_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("WITH source AS (")
    ]
    region_link_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_observation_region")
    ]
    region_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_region_revision(")
    ]
    membership_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_region_target")
    ]
    assert len(observation_batches) == 1
    assert len(observation_batches[0]) == 1
    assert len(region_link_batches) == 1
    assert len(region_link_batches[0]) == 1
    assert region_link_batches[0][0][5] == "region-revision:home:v1"
    assert len(region_batches) == 1
    assert region_batches[0][0][0] == "region-revision:home:v1"
    assert len(membership_batches) == 1
    assert {
        (row[0], row[1]) for row in membership_batches[0]
    } == {
        ("home", "region-revision:home:v1"),
        ("child", "region-revision:home:v1"),
    }
    assert result.written_rows["observations"] == 1
    assert result.written_rows["observation_region_links"] == 1
    assert result.written_rows["regions"] == 1
    assert result.written_rows["region_targets"] == 2


@pytest.mark.asyncio
async def test_writer_stores_one_physical_station_measurement_for_two_targets():
    cycle, nowcast = _station_cycle_and_nowcast(100.0, "home", "child")
    factory = ConnectionFactory()

    result = await ThinPostGISWriter(
        _settings(),
        connect_factory=factory,
    ).write_cycle(
        cycle,
        nowcast,
        region_plan=_shared_home_region_plan(100.0),
    )

    batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_station_measurement(")
    ]
    assert len(batches) == 1
    assert len(batches[0]) == 1
    assert "home" not in batches[0][0]
    assert "child" not in batches[0][0]
    assert result.written_rows["station_measurements"] == 1
    station_region_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_station_measurement_region")
    ]
    assert len(station_region_batches) == 1
    assert len(station_region_batches[0]) == 1
    assert station_region_batches[0][0][4] == "region-revision:home:v1"
    assert result.written_rows["station_measurement_region_links"] == 1
    station_sql = next(
        sql for sql, _ in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_station_measurement(")
    )
    assert "EXCLUDED.received_at >= stv3_station_measurement.received_at" in station_sql


@pytest.mark.asyncio
async def test_writer_links_one_shared_fact_to_two_region_revisions():
    factory = ConnectionFactory()
    cycle, nowcast = _shared_source_cycle_and_nowcast(100.0)

    result = await ThinPostGISWriter(
        _settings(), connect_factory=factory,
    ).write_cycle(cycle, nowcast, region_plan=_split_region_plan(100.0))

    observation_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("WITH source AS (")
    ]
    link_batches = [
        rows for sql, rows in factory.batch_calls
        if sql.startswith("INSERT INTO stv3_observation_region")
    ]
    assert len(observation_batches[0]) == 1
    assert len(link_batches[0]) == 2
    assert {row[5] for row in link_batches[0]} == {
        "region-revision:home:v1",
        "region-revision:child:v1",
    }
    assert result.written_rows["observations"] == 1
    assert result.written_rows["observation_region_links"] == 2


@pytest.mark.asyncio
async def test_writer_closes_absent_dynamic_region_lifecycle():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _shared_source_cycle_and_nowcast(100.0)
    await writer.write_cycle(
        cycle,
        nowcast,
        region_plan=_split_region_plan(100.0),
    )
    home_only = _shared_home_region_plan(200.0)
    await writer.write_cycle(cycle, nowcast, region_plan=home_only)

    closure_sql = "\n".join(
        sql for sql, _parameters in factory.statements
        if sql.startswith("UPDATE stv3_region_")
    )
    assert "UPDATE stv3_region_target" in closure_sql
    assert "UPDATE stv3_region_revision" in closure_sql
    assert "UPDATE stv3_region_instance" in closure_sql
    assert "valid_until=to_timestamp" in closure_sql


@pytest.mark.asyncio
@pytest.mark.parametrize("provider_id", ["netatmo", "meteolux"])
async def test_writer_auto_registers_unknown_ground_station_before_measurement(provider_id):
    cycle, nowcast = _station_cycle_and_nowcast(100.0, "home", provider_id=provider_id)
    statements = []

    result = await ThinPostGISWriter(
        _settings(),
        connect_factory=lambda **kwargs: FakeConnection(
            statements,
            known_stations=set(),
        ),
    ).write_cycle(cycle, nowcast)

    assert result.status == "current"
    assert result.written_rows["station_measurements"] == 1
    assert result.written_rows["observations"] == 0
    assert any(
        sql.startswith("INSERT INTO stv3_ground_station")
        for sql, _ in statements
    )
    assert any(
        sql.startswith("INSERT INTO stv3_station_measurement")
        for sql, _ in statements
    )


@pytest.mark.asyncio
async def test_writer_does_not_auto_register_non_ground_provider_as_station():
    cycle, nowcast = _station_cycle_and_nowcast(
        100.0,
        "home",
        provider_id="untrusted_model",
    )
    statements = []

    result = await ThinPostGISWriter(
        _settings(),
        connect_factory=lambda **kwargs: FakeConnection(
            statements,
            known_stations=set(),
        ),
    ).write_cycle(cycle, nowcast)

    assert result.status == "current"
    assert result.written_rows["station_measurements"] == 0
    assert result.written_rows["observations"] == 1
    assert not any(
        sql.startswith("INSERT INTO stv3_ground_station")
        for sql, _ in statements
    )


@pytest.mark.asyncio
async def test_writer_keeps_nonstation_radar_on_generic_observation_route():
    cycle, nowcast = _cycle_and_nowcast(100.0)
    factory = ConnectionFactory()

    await ThinPostGISWriter(
        _settings(),
        connect_factory=factory,
    ).write_cycle(cycle, nowcast)

    assert any(
        sql.startswith("WITH source AS (")
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
        if sql.startswith("WITH source AS (")
    ]
    assert result.status == "current"
    assert len(observation_rows) == 1
    assert observation_rows[0][0] == "goes19_rrqpe"
    assert observation_rows[0][2] == "rain_rate"
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
        if item[0].startswith("WITH source AS (")
    ]
    assert len(observation_inserts) == 2
    assert all("ON CONFLICT" in sql and "DO UPDATE" in sql for sql, _ in observation_inserts)
    assert observation_inserts[0][1] == observation_inserts[1][1]
    geometry = json.loads(observation_inserts[0][1][4])
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
        speed_kmh=60.0, heading_deg=90.0, motion_ready=True,
    )
    nowcast = ThinNowcastRuntime(targets={"home": (0.0, 0.10)}).advance(
        cycle, tracked_cells=(cell,),
    )

    result = await writer.write_cycle(cycle, nowcast)

    inserts = [
        item for item in factory.statements
        if item[0].startswith("INSERT INTO stv3_target_passage(")
    ]
    assert result.written_rows["target_passages"] == 1
    assert len(inserts) == 1
    assert inserts[0][1][4] < inserts[0][1][5]
    assert inserts[0][1][6] == "translated_footprint"


@pytest.mark.asyncio
async def test_provisional_passage_is_immutable_without_fabricated_exit() -> None:
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _cycle_and_nowcast(100.0)
    provisional = TargetPassage(
        target_id="home",
        evaluated_at=nowcast.evaluated_at,
        cell_id="system:a",
        provider_id="fusion:home",
        entry_at=200.0,
        exit_at=None,
        method="advected_labelled_qpe",
        predicted_intensity_mm_h=2.0,
        footprint=MappingProxyType({
            "type": "Point", "coordinates": [4.35, 50.85],
        }),
        right_censored=True,
        interval_ordinal=1,
        quantity="precipitation_rate",
        unit="mm/h",
        field_fingerprint="e" * 64,
        passage_system_id="passage:system-a",
        passage_status="candidate",
    )
    nowcast = replace(nowcast, target_passages=(provisional,))

    first = await writer.write_cycle(cycle, nowcast)
    second = await writer.write_cycle(cycle, nowcast)

    issue_rows = [
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_target_forecast_issue")
    ]
    passage_rows = [
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_target_passage_issue")
    ]
    geometric_rows = [
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_target_passage(")
    ]
    assert first.status == second.status == "current"
    assert len(issue_rows) == 2
    assert issue_rows[0][0] == issue_rows[1][0]
    assert len(passage_rows) == 2
    assert passage_rows[0][0] == passage_rows[1][0]
    assert passage_rows[0][7] is None
    assert geometric_rows == []


@pytest.mark.asyncio
async def test_writer_keeps_base_projection_current_before_migration_0008() -> None:
    factory = ConnectionFactory(scientific_passage_schema=False)
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, nowcast = _cycle_and_nowcast(100.0)

    result = await writer.write_cycle(cycle, nowcast)

    assert result.status == "current"
    assert result.written_rows["target_forecasts"] == 1
    assert result.written_rows["forecast_issues"] == 0
    assert result.written_rows["passage_issues"] == 0
    assert any(
        sql.startswith("INSERT INTO stv3_target_forecast(")
        for sql, _parameters in factory.statements
    )
    assert not any(
        sql.startswith("INSERT INTO stv3_target_forecast_issue")
        for sql, _parameters in factory.statements
    )


@pytest.mark.asyncio
async def test_writer_keeps_revision_on_cells_tracks_and_target_forecasts():
    factory = ConnectionFactory()
    writer = ThinPostGISWriter(_settings(), connect_factory=factory)
    cycle, _unused = _cycle_and_nowcast(100.0)
    cell = StormCell(
        cell_id="regional-cell", provider_id="opera_radar", observed_at=100.0,
        latitude=0.0, longitude=0.0,
        geometry={"type": "Polygon", "coordinates": [[
            [-0.01, -0.01], [0.01, -0.01], [0.01, 0.01],
            [-0.01, 0.01], [-0.01, -0.01],
        ]]},
        precipitation_rate_mm_h=4.0, reflectivity_dbz=None, radar_level=None,
        velocity_east_kmh=60.0, velocity_north_kmh=0.0,
        speed_kmh=60.0, heading_deg=90.0, motion_ready=True,
    )
    positions = {"home": (0.0, 0.10), "child": (0.0, 0.20)}
    nowcast = ThinNowcastRuntime(targets=positions).advance(
        cycle, tracked_cells=(cell,),
    )

    await writer.write_cycle(
        cycle,
        nowcast,
        region_plan=_single_region_plan(100.0, positions),
    )

    cell_row = next(
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_storm_cell")
    )
    track_row = next(
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_track_point")
    )
    forecast_rows = {
        row[0]: row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_target_forecast(")
    }
    assert cell_row[-1] == "region-revision:test:v1"
    assert track_row[-2] == "region-revision:test:v1"
    assert set(forecast_rows) == {"home", "child"}
    assert {row[-1] for row in forecast_rows.values()} == {
        "region-revision:test:v1"
    }
    for target_id, forecast in nowcast.target_results.items():
        assert forecast_rows[target_id][4] == forecast.precipitation_rate_mm_h
        assert forecast_rows[target_id][5] == forecast.eta_minutes


@pytest.mark.asyncio
async def test_writer_uses_cell_source_region_when_active_regions_overlap():
    factory = ConnectionFactory()
    cycle, _unused = _cycle_and_nowcast(100.0)
    plan = _split_region_plan(100.0)
    plan = replace(
        plan,
        regions=(
            plan.regions[0],
            replace(plan.regions[1], center_longitude=15.81),
        ),
    )
    cell = StormCell(
        cell_id="external-cell",
        provider_id="opera_radar",
        observed_at=100.0,
        latitude=38.35,
        longitude=15.8,
        geometry={"type": "Point", "coordinates": [15.8, 38.35]},
        precipitation_rate_mm_h=4.0,
        reflectivity_dbz=None,
        radar_level=None,
        velocity_east_kmh=0.0,
        velocity_north_kmh=0.0,
        speed_kmh=0.0,
        heading_deg=None,
        motion_ready=False,
        region_id="region:child",
    )
    nowcast = ThinNowcastRuntime(targets={}).advance(
        cycle,
        tracked_cells=(cell,),
    )

    await ThinPostGISWriter(
        _settings(), connect_factory=factory,
    ).write_cycle(cycle, nowcast, region_plan=plan)

    cell_row = next(
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_storm_cell")
    )
    track_row = next(
        row for sql, row in factory.statements
        if sql.startswith("INSERT INTO stv3_track_point")
    )
    assert cell_row[-1] == "region-revision:child:v1"
    assert "region-revision:child:v1" in track_row


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
