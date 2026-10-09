from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from storm_tracker_worker.thin_station_bootstrap import (
    BootstrapBudget,
    DiscoveryPage,
    NetatmoTileSource,
    NetatmoWorldCatalogRunner,
    RetryableDiscoveryError,
    StationCatalogBootstrap,
    TileSubdivisionRequired,
    geographic_tiles,
    load_wunderground_seed_stations,
    parse_metar_station_cache,
    parse_netatmo_public_stations,
    subdivide_geographic_tile,
    world_netatmo_tiles,
)
from storm_tracker_worker.thin_station_catalog import DiscoveryCheckpoint


NOW = 1_800_000_000.0


def test_complete_official_metar_cache_normalizes_global_station_metadata():
    payload = gzip.compress(json.dumps([
        {
            "id": "EBBR",
            "icaoId": "EBBR",
            "site": "Brussels Airport",
            "lat": 50.901,
            "lon": 4.484,
            "country": "BE",
        },
        {
            "id": "YSSY",
            "icaoId": "YSSY",
            "site": "Sydney Airport",
            "lat": -33.946,
            "lon": 151.177,
            "country": "AU",
        },
    ]).encode())

    stations = parse_metar_station_cache(payload, discovered_at=NOW)

    assert [item.station_id for item in stations] == ["EBBR", "YSSY"]
    assert stations[0].provider_id == "metar"
    assert {"temperature", "pressure", "wind_speed"} <= stations[0].capabilities
    assert stations[1].longitude == 151.177


def test_netatmo_public_response_inventories_station_capabilities():
    stations = parse_netatmo_public_stations({
        "body": [{
            "_id": "70:ee:50:29:59:56",
            "place": {"location": [4.42, 51.05]},
            "measures": {
                "rain": {"rain_live": 0.0},
                "wind": {"wind_strength": 8, "wind_angle": 240},
                "outdoor": {
                    "type": ["temperature", "humidity", "pressure"],
                    "res": {"1800000000": [12.4, 82, 1008.2]},
                },
            },
        }],
    }, discovered_at=NOW)

    assert len(stations) == 1
    assert stations[0].station_id == "70:ee:50:29:59:56"
    assert stations[0].capabilities == frozenset({
        "rain_rate", "wind_speed", "wind_direction",
        "temperature", "relative_humidity", "pressure",
    })


def test_geographic_tiles_cover_bounds_once_with_stable_ids():
    tiles = geographic_tiles(
        south=50.0, north=51.0, west=4.0, east=5.0, tile_degrees=0.5,
    )

    assert [item.tile_id for item in tiles] == [
        "s+050000_n+050500_w+004000_e+004500",
        "s+050000_n+050500_w+004500_e+005000",
        "s+050500_n+051000_w+004000_e+004500",
        "s+050500_n+051000_w+004500_e+005000",
    ]


def test_dense_world_tile_subdivision_is_stable_and_gap_free():
    parent = geographic_tiles(
        south=50.0, north=51.0, west=4.0, east=5.0, tile_degrees=1.0,
    )[0]

    children = subdivide_geographic_tile(parent)

    assert [item.tile_id for item in children] == [
        "s+050000_n+050500_w+004000_e+004500",
        "s+050000_n+050500_w+004500_e+005000",
        "s+050500_n+051000_w+004000_e+004500",
        "s+050500_n+051000_w+004500_e+005000",
    ]


def test_world_netatmo_inventory_uses_one_degree_tiles_everywhere():
    tiles = world_netatmo_tiles()

    assert len(tiles) == 61_200
    assert (tiles[0].south, tiles[0].north, tiles[0].west, tiles[0].east) == (
        -85.0, -84.0, -180.0, -179.0,
    )
    assert (tiles[-1].south, tiles[-1].north, tiles[-1].west, tiles[-1].east) == (
        84.0, 85.0, 179.0, 180.0,
    )


class _Repository:
    def __init__(self, checkpoints=()):
        self.checkpoints = {
            (item.provider_id, item.tile_id): item for item in checkpoints
        }
        self.stations = {}
        self.saved = []

    def load_checkpoint(self, provider_id, tile_id):
        return self.checkpoints.get((provider_id, tile_id))

    def upsert_stations(self, stations):
        for station in stations:
            self.stations[station.station_key] = station
        return len(tuple(stations))

    def save_checkpoint(self, checkpoint):
        self.checkpoints[(checkpoint.provider_id, checkpoint.tile_id)] = checkpoint
        self.saved.append(checkpoint)


class _Source:
    provider_id = "netatmo"

    def __init__(self):
        self.calls = []

    async def discover(self, tile, *, cursor):
        self.calls.append((tile.tile_id, cursor))
        station_id = "station-" + tile.tile_id
        return DiscoveryPage(
            stations=parse_netatmo_public_stations({
                "body": [{
                    "_id": station_id,
                    "place": {"location": [tile.west, tile.south]},
                    "measures": {"rain": {"rain_live": 0.0}},
                }],
            }, discovered_at=NOW),
            next_cursor=None,
            completed=True,
            response_bytes=512,
        )


class _AdaptiveSource:
    provider_id = "netatmo"

    def __init__(self, parent_tile_id):
        self.parent_tile_id = parent_tile_id
        self.calls = []

    async def discover(self, tile, *, cursor):
        self.calls.append((tile.tile_id, cursor))
        if tile.tile_id == self.parent_tile_id:
            raise TileSubdivisionRequired("netatmo_tile_too_dense")
        station_id = "station-" + tile.tile_id
        return DiscoveryPage(
            stations=parse_netatmo_public_stations({
                "body": [{
                    "_id": station_id,
                    "place": {"location": [tile.west, tile.south]},
                    "measures": {"rain": {"rain_live": 0.0}},
                }],
            }, discovered_at=NOW),
            next_cursor=None,
            completed=True,
            response_bytes=512,
        )


class _RetrySource(_Source):
    def __init__(self):
        super().__init__()
        self.failed = False

    async def discover(self, tile, *, cursor):
        if not self.failed:
            self.failed = True
            raise RetryableDiscoveryError("netatmo_http_429", retry_after_seconds=60.0)
        return await super().discover(tile, cursor=cursor)


@pytest.mark.asyncio
async def test_bootstrap_skips_completed_tiles_and_resumes_partial_checkpoint():
    tiles = geographic_tiles(
        south=50.0, north=50.5, west=4.0, east=5.0, tile_degrees=0.5,
    )
    repository = _Repository((
        DiscoveryCheckpoint(
            provider_id="netatmo", tile_id=tiles[0].tile_id, cursor=None,
            completed=True, station_count=4, updated_at=NOW,
        ),
        DiscoveryCheckpoint(
            provider_id="netatmo", tile_id=tiles[1].tile_id, cursor="page-2",
            completed=False, station_count=7, updated_at=NOW,
        ),
    ))
    source = _Source()

    report = await StationCatalogBootstrap(
        repository=repository, source=source, clock=lambda: NOW,
    ).run(tiles, budget=BootstrapBudget(maximum_calls=10, maximum_bytes=4096))

    assert source.calls == [(tiles[1].tile_id, "page-2")]
    assert report.completed_tiles == 1
    assert report.skipped_tiles == 1
    assert report.remaining_tiles == 0
    assert repository.saved[-1].station_count == 8


@pytest.mark.asyncio
async def test_bootstrap_budget_stops_cleanly_and_remains_resumable():
    tiles = geographic_tiles(
        south=50.0, north=51.0, west=4.0, east=5.0, tile_degrees=0.5,
    )
    repository = _Repository()
    source = _Source()

    report = await StationCatalogBootstrap(
        repository=repository, source=source, clock=lambda: NOW,
    ).run(tiles, budget=BootstrapBudget(maximum_calls=2, maximum_bytes=4096))

    assert report.status == "budget_exhausted"
    assert report.calls == 2
    assert report.completed_tiles == 2
    assert report.remaining_tiles == 2
    assert len(repository.saved) == 2


@pytest.mark.asyncio
async def test_bootstrap_can_rate_limit_calls_without_delaying_first_call():
    tiles = geographic_tiles(
        south=50.0, north=50.5, west=4.0, east=5.5, tile_degrees=0.5,
    )
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    report = await StationCatalogBootstrap(
        repository=_Repository(),
        source=_Source(),
        clock=lambda: NOW,
        minimum_call_interval_seconds=8.0,
        sleep=fake_sleep,
    ).run(tiles, budget=BootstrapBudget(maximum_calls=3, maximum_bytes=4096))

    assert report.calls == 3
    assert sleeps == [8.0, 8.0]


@pytest.mark.asyncio
async def test_bootstrap_retries_quota_response_without_losing_checkpoint():
    tile = geographic_tiles(
        south=50.0, north=50.5, west=4.0, east=4.5, tile_degrees=0.5,
    )[0]
    sleeps = []

    async def fake_sleep(seconds):
        sleeps.append(seconds)

    report = await StationCatalogBootstrap(
        repository=_Repository(),
        source=_RetrySource(),
        sleep=fake_sleep,
    ).run((tile,), budget=BootstrapBudget(maximum_calls=3, maximum_bytes=4096))

    assert report.status == "complete"
    assert report.calls == 2
    assert sleeps == [60.0]


@pytest.mark.asyncio
async def test_bootstrap_subdivides_dense_tile_and_completes_parent_checkpoint():
    parent = geographic_tiles(
        south=50.0, north=51.0, west=4.0, east=5.0, tile_degrees=1.0,
    )[0]
    repository = _Repository()
    source = _AdaptiveSource(parent.tile_id)

    report = await StationCatalogBootstrap(
        repository=repository,
        source=source,
        minimum_tile_degrees=0.125,
    ).run((parent,), budget=BootstrapBudget(maximum_calls=10, maximum_bytes=4096))

    assert report.status == "complete"
    assert report.calls == 5
    assert report.remaining_tiles == 0
    assert len(repository.stations) == 4
    assert repository.load_checkpoint("netatmo", parent.tile_id).completed is True


@pytest.mark.asyncio
async def test_adaptive_bootstrap_resumes_completed_children_after_budget_stop():
    parent = geographic_tiles(
        south=50.0, north=51.0, west=4.0, east=5.0, tile_degrees=1.0,
    )[0]
    repository = _Repository()

    first_source = _AdaptiveSource(parent.tile_id)
    first = await StationCatalogBootstrap(
        repository=repository,
        source=first_source,
        minimum_tile_degrees=0.125,
    ).run((parent,), budget=BootstrapBudget(maximum_calls=2, maximum_bytes=4096))

    assert first.status == "budget_exhausted"
    assert repository.load_checkpoint("netatmo", parent.tile_id) is None
    completed_children = {
        checkpoint.tile_id for checkpoint in repository.saved
        if checkpoint.completed
    }
    assert len(completed_children) == 1

    second_source = _AdaptiveSource(parent.tile_id)
    second = await StationCatalogBootstrap(
        repository=repository,
        source=second_source,
        minimum_tile_degrees=0.125,
    ).run((parent,), budget=BootstrapBudget(maximum_calls=10, maximum_bytes=4096))

    assert second.status == "complete"
    assert second.remaining_tiles == 0
    assert completed_children.isdisjoint({
        tile_id for tile_id, _cursor in second_source.calls
        if tile_id != parent.tile_id
    })


@pytest.mark.asyncio
async def test_netatmo_tile_source_requests_a_fresh_provider_token_per_call():
    tiles = geographic_tiles(
        south=50.0, north=50.5, west=4.0, east=5.0, tile_degrees=0.5,
    )
    issued = iter(("token-one", "token-two"))
    requests = []

    async def token_provider():
        return next(issued)

    async def fetch_json(url, *, params, headers, maximum_bytes):
        requests.append((url, params, headers, maximum_bytes))
        return {"body": []}

    source = NetatmoTileSource(
        fetch_json=fetch_json,
        access_token_provider=token_provider,
    )
    await source.discover(tiles[0], cursor=None)
    await source.discover(tiles[1], cursor=None)

    assert [item[2]["Authorization"] for item in requests] == [
        "Bearer token-one", "Bearer token-two",
    ]


@pytest.mark.asyncio
async def test_netatmo_503_requests_adaptive_subdivision():
    tile = geographic_tiles(
        south=50.0, north=51.0, west=4.0, east=5.0, tile_degrees=1.0,
    )[0]

    async def fetch_json(*_args, **_kwargs):
        raise RuntimeError("http_status_503")

    source = NetatmoTileSource(
        fetch_json=fetch_json,
        access_token="token",
    )

    with pytest.raises(TileSubdivisionRequired, match="netatmo_http_503"):
        await source.discover(tile, cursor=None)


@pytest.mark.asyncio
async def test_netatmo_429_requests_quota_backoff():
    tile = geographic_tiles(
        south=50.0, north=50.5, west=4.0, east=4.5, tile_degrees=0.5,
    )[0]

    async def fetch_json(*_args, **_kwargs):
        raise RuntimeError("http_status_429")

    source = NetatmoTileSource(fetch_json=fetch_json, access_token="token")

    with pytest.raises(RetryableDiscoveryError, match="netatmo_http_429"):
        await source.discover(tile, cursor=None)


@pytest.mark.asyncio
async def test_world_runner_writes_completion_marker_and_skips_repeat_scan():
    tile = geographic_tiles(
        south=50.0, north=50.5, west=4.0, east=4.5, tile_degrees=0.5,
    )[0]
    repository = _Repository()
    source = _Source()
    runner = NetatmoWorldCatalogRunner(
        repository=repository,
        source=source,
        tiles=(tile,),
        minimum_call_interval_seconds=10.0,
        sleep=lambda _seconds: pytest.fail("one call must not sleep"),
    )

    first = await runner.run(
        budget=BootstrapBudget(maximum_calls=2, maximum_bytes=4096),
    )
    second = await runner.run(
        budget=BootstrapBudget(maximum_calls=2, maximum_bytes=4096),
    )

    assert first.status == "complete"
    assert second.status == "already_complete"
    assert len(source.calls) == 1


@pytest.mark.asyncio
async def test_world_runner_resumes_from_persisted_root_cursor():
    tiles = geographic_tiles(
        south=50.0, north=50.5, west=4.0, east=5.5, tile_degrees=0.5,
    )
    repository = _Repository()
    first_source = _Source()
    first_runner = NetatmoWorldCatalogRunner(
        repository=repository,
        source=first_source,
        tiles=tiles,
        minimum_call_interval_seconds=0.0,
    )

    first = await first_runner.run(
        budget=BootstrapBudget(maximum_calls=2, maximum_bytes=4096),
    )

    assert first.status == "budget_exhausted"
    marker = repository.load_checkpoint(
        "netatmo", NetatmoWorldCatalogRunner.COMPLETION_TILE_ID,
    )
    assert marker.completed is False
    assert marker.cursor == "2"

    second_source = _Source()
    second = await NetatmoWorldCatalogRunner(
        repository=repository,
        source=second_source,
        tiles=tiles,
        minimum_call_interval_seconds=0.0,
    ).run(budget=BootstrapBudget(maximum_calls=2, maximum_bytes=4096))

    assert second.status == "complete"
    assert [tile_id for tile_id, _cursor in second_source.calls] == [
        tiles[2].tile_id,
    ]


def test_wunderground_seed_inventory_needs_no_api_key_and_contains_no_removed_station():
    path = (
        Path(__file__).parents[1]
        / "storm_tracker_worker"
        / "wunderground_seed_stations.json"
    )

    stations = load_wunderground_seed_stations(path, discovered_at=NOW)

    assert {item.station_id for item in stations} == {
        "IWILLE87", "IMECHE70", "IMECHE17", "IRUMST20", "IKAPEL108",
        "IMECHE71", "IMECHE79", "IDUFFE19", "IBOOM10",
    }
    assert "IMECHE89" not in {item.station_id for item in stations}
    assert all(item.provider_id == "wunderground" for item in stations)
    serialized = json.dumps([item.station_id for item in stations]).lower()
    assert "api_key" not in serialized
    assert "password" not in serialized
