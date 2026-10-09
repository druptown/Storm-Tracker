"""Resumable, rate-bounded offline station catalog bootstrap."""

from __future__ import annotations

from dataclasses import dataclass
import gzip
import json
import math
from pathlib import Path
import time
from typing import Any, Awaitable, Callable, Iterable, Mapping, Protocol

from .thin_station_catalog import DiscoveryCheckpoint, GroundStation


METAR_STATION_CACHE_URL = (
    "https://aviationweather.gov/data/cache/stations.cache.json.gz"
)
NETATMO_PUBLIC_DATA_URL = "https://api.netatmo.com/api/getpublicdata"


@dataclass(frozen=True, slots=True)
class GeographicTile:
    tile_id: str
    south: float
    north: float
    west: float
    east: float


@dataclass(frozen=True, slots=True)
class DiscoveryPage:
    stations: tuple[GroundStation, ...]
    next_cursor: str | None
    completed: bool
    response_bytes: int


@dataclass(frozen=True, slots=True)
class BootstrapBudget:
    maximum_calls: int
    maximum_bytes: int

    def __post_init__(self) -> None:
        if (
            isinstance(self.maximum_calls, bool)
            or self.maximum_calls < 1
            or isinstance(self.maximum_bytes, bool)
            or self.maximum_bytes < 1
        ):
            raise ValueError("bootstrap_budget_invalid")


@dataclass(frozen=True, slots=True)
class BootstrapReport:
    provider_id: str
    status: str
    calls: int
    response_bytes: int
    stations_written: int
    completed_tiles: int
    skipped_tiles: int
    remaining_tiles: int


class BootstrapRepository(Protocol):
    def load_checkpoint(
        self, provider_id: str, tile_id: str,
    ) -> DiscoveryCheckpoint | None: ...

    def upsert_stations(self, stations: Iterable[GroundStation]) -> int: ...

    def save_checkpoint(self, checkpoint: DiscoveryCheckpoint) -> None: ...


class BootstrapSource(Protocol):
    provider_id: str

    async def discover(
        self, tile: GeographicTile, *, cursor: str | None,
    ) -> DiscoveryPage: ...


def _finite(value: object, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label}_invalid") from error
    if not math.isfinite(result):
        raise ValueError(f"{label}_invalid")
    return result


def _tile_component(prefix: str, value: float) -> str:
    scaled = round(value * 1000.0)
    sign = "+" if scaled >= 0 else "-"
    return f"{prefix}{sign}{abs(scaled):06d}"


def geographic_tiles(
    *,
    south: float,
    north: float,
    west: float,
    east: float,
    tile_degrees: float,
) -> tuple[GeographicTile, ...]:
    south_value = _finite(south, "tiles_south")
    north_value = _finite(north, "tiles_north")
    west_value = _finite(west, "tiles_west")
    east_value = _finite(east, "tiles_east")
    step = _finite(tile_degrees, "tiles_step")
    if (
        not -85.0 <= south_value < north_value <= 85.0
        or not -180.0 <= west_value < east_value <= 180.0
        or step <= 0.0
        or step > 10.0
    ):
        raise ValueError("tiles_bounds_invalid")
    result: list[GeographicTile] = []
    latitude = south_value
    while latitude < north_value - 1e-12:
        tile_north = min(north_value, latitude + step)
        longitude = west_value
        while longitude < east_value - 1e-12:
            tile_east = min(east_value, longitude + step)
            tile_id = "_".join((
                _tile_component("s", latitude),
                _tile_component("n", tile_north),
                _tile_component("w", longitude),
                _tile_component("e", tile_east),
            ))
            result.append(GeographicTile(
                tile_id=tile_id,
                south=latitude,
                north=tile_north,
                west=longitude,
                east=tile_east,
            ))
            longitude = tile_east
        latitude = tile_north
    return tuple(result)


_METAR_CAPABILITIES = (
    "temperature",
    "relative_humidity",
    "pressure",
    "wind_speed",
    "wind_gust",
    "wind_direction",
)


def parse_metar_station_cache(
    payload: bytes,
    *,
    discovered_at: float,
) -> tuple[GroundStation, ...]:
    body = gzip.decompress(payload) if payload.startswith(b"\x1f\x8b") else payload
    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("metar_station_cache_invalid") from error
    if not isinstance(decoded, list):
        raise ValueError("metar_station_cache_invalid")
    stations: dict[str, GroundStation] = {}
    for row in decoded:
        if not isinstance(row, Mapping):
            continue
        station_id = row.get("icaoId") or row.get("id")
        latitude = row.get("lat")
        longitude = row.get("lon")
        if not isinstance(station_id, str) or latitude is None or longitude is None:
            continue
        try:
            station = GroundStation.create(
                provider_id="metar",
                station_id=station_id,
                latitude=float(latitude),
                longitude=float(longitude),
                capabilities=_METAR_CAPABILITIES,
                discovered_at=discovered_at,
                last_seen_at=discovered_at,
                quality_score=0.9,
            )
        except (TypeError, ValueError):
            continue
        stations[station.station_key] = station
    return tuple(stations[key] for key in sorted(stations))


def _netatmo_capabilities(measures: object) -> tuple[str, ...]:
    capabilities: set[str] = set()
    for measure in measures.values() if isinstance(measures, Mapping) else ():
        if not isinstance(measure, Mapping):
            continue
        for field, capability in (
            ("rain_live", "rain_rate"),
            ("rain_60min", "rain_accumulation"),
            ("wind_strength", "wind_speed"),
            ("gust_strength", "wind_gust"),
            ("wind_angle", "wind_direction"),
        ):
            if measure.get(field) is not None:
                capabilities.add(capability)
        for name in measure.get("type", ()) if isinstance(measure.get("type"), list) else ():
            mapped = {
                "temperature": "temperature",
                "humidity": "relative_humidity",
                "pressure": "pressure",
            }.get(name)
            if mapped:
                capabilities.add(mapped)
    return tuple(sorted(capabilities))


def parse_netatmo_public_stations(
    payload: object,
    *,
    discovered_at: float,
) -> tuple[GroundStation, ...]:
    rows = payload.get("body") if isinstance(payload, Mapping) else None
    if not isinstance(rows, list):
        raise ValueError("netatmo_station_payload_invalid")
    stations: dict[str, GroundStation] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        place = row.get("place")
        location = place.get("location") if isinstance(place, Mapping) else None
        station_id = row.get("_id")
        if (
            not isinstance(station_id, str)
            or not isinstance(location, list)
            or len(location) != 2
        ):
            continue
        capabilities = _netatmo_capabilities(row.get("measures"))
        if not capabilities:
            continue
        try:
            station = GroundStation.create(
                provider_id="netatmo",
                station_id=station_id,
                latitude=float(location[1]),
                longitude=float(location[0]),
                capabilities=capabilities,
                discovered_at=discovered_at,
                last_seen_at=discovered_at,
                quality_score=0.8,
            )
        except (TypeError, ValueError):
            continue
        stations[station.station_key] = station
    return tuple(stations[key] for key in sorted(stations))


def load_wunderground_seed_stations(
    path: str | Path,
    *,
    discovered_at: float,
) -> tuple[GroundStation, ...]:
    try:
        rows = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("wunderground_seed_invalid") from error
    if not isinstance(rows, list):
        raise ValueError("wunderground_seed_invalid")
    stations: list[GroundStation] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("wunderground_seed_invalid")
        stations.append(GroundStation.create(
            provider_id="wunderground",
            station_id=str(row.get("station_id", "")),
            latitude=row.get("latitude"),
            longitude=row.get("longitude"),
            capabilities=row.get("capabilities", ()),
            discovered_at=discovered_at,
            last_seen_at=discovered_at,
            quality_score=float(row.get("quality_score", 0.7)),
        ))
    return tuple(sorted(stations, key=lambda item: item.station_key))


class StationCatalogBootstrap:
    def __init__(
        self,
        *,
        repository: BootstrapRepository,
        source: BootstrapSource,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._repository = repository
        self._source = source
        self._clock = clock

    async def run(
        self,
        tiles: Iterable[GeographicTile],
        *,
        budget: BootstrapBudget,
    ) -> BootstrapReport:
        inventory = tuple(tiles)
        calls = 0
        response_bytes = 0
        stations_written = 0
        completed_tiles = 0
        skipped_tiles = 0
        completed_ids: set[str] = set()
        status = "complete"
        for tile in inventory:
            checkpoint = self._repository.load_checkpoint(
                self._source.provider_id, tile.tile_id,
            )
            if checkpoint is not None and checkpoint.completed:
                skipped_tiles += 1
                completed_ids.add(tile.tile_id)
                continue
            cursor = None if checkpoint is None else checkpoint.resume_cursor
            station_count = 0 if checkpoint is None else checkpoint.station_count
            while True:
                if calls >= budget.maximum_calls or response_bytes >= budget.maximum_bytes:
                    status = "budget_exhausted"
                    break
                page = await self._source.discover(tile, cursor=cursor)
                calls += 1
                response_bytes += page.response_bytes
                stations_written += self._repository.upsert_stations(page.stations)
                station_count += len({item.station_key for item in page.stations})
                checkpoint = DiscoveryCheckpoint(
                    provider_id=self._source.provider_id,
                    tile_id=tile.tile_id,
                    cursor=page.next_cursor,
                    completed=page.completed,
                    station_count=station_count,
                    updated_at=float(self._clock()),
                )
                self._repository.save_checkpoint(checkpoint)
                if page.completed:
                    completed_tiles += 1
                    completed_ids.add(tile.tile_id)
                    break
                cursor = page.next_cursor
            if status == "budget_exhausted":
                break
        remaining = sum(1 for item in inventory if item.tile_id not in completed_ids)
        return BootstrapReport(
            provider_id=self._source.provider_id,
            status=status,
            calls=calls,
            response_bytes=response_bytes,
            stations_written=stations_written,
            completed_tiles=completed_tiles,
            skipped_tiles=skipped_tiles,
            remaining_tiles=remaining,
        )


class SinglePageSource:
    """Adapter for an already downloaded complete provider snapshot."""

    def __init__(
        self,
        *,
        provider_id: str,
        stations: tuple[GroundStation, ...],
        response_bytes: int,
    ) -> None:
        self.provider_id = provider_id
        self._stations = stations
        self._response_bytes = response_bytes

    async def discover(
        self, tile: GeographicTile, *, cursor: str | None,
    ) -> DiscoveryPage:
        del tile, cursor
        return DiscoveryPage(
            stations=self._stations,
            next_cursor=None,
            completed=True,
            response_bytes=self._response_bytes,
        )


class NetatmoTileSource:
    provider_id = "netatmo"

    def __init__(
        self,
        *,
        fetch_json: Callable[..., Awaitable[Any]],
        access_token: str,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not access_token:
            raise ValueError("netatmo_access_token_missing")
        self._fetch_json = fetch_json
        self._access_token = access_token
        self._clock = clock

    async def discover(
        self, tile: GeographicTile, *, cursor: str | None,
    ) -> DiscoveryPage:
        if cursor is not None:
            raise ValueError("netatmo_cursor_unsupported")
        payload = await self._fetch_json(
            NETATMO_PUBLIC_DATA_URL,
            params={
                "lat_ne": f"{tile.north:.5f}",
                "lon_ne": f"{tile.east:.5f}",
                "lat_sw": f"{tile.south:.5f}",
                "lon_sw": f"{tile.west:.5f}",
                "filter": "true",
            },
            headers={"Authorization": f"Bearer {self._access_token}"},
            maximum_bytes=16 * 1024 * 1024,
        )
        encoded_size = len(json.dumps(payload, separators=(",", ":")).encode())
        return DiscoveryPage(
            stations=parse_netatmo_public_stations(
                payload, discovered_at=float(self._clock()),
            ),
            next_cursor=None,
            completed=True,
            response_bytes=encoded_size,
        )


__all__ = [
    "BootstrapBudget",
    "BootstrapReport",
    "DiscoveryPage",
    "GeographicTile",
    "METAR_STATION_CACHE_URL",
    "NETATMO_PUBLIC_DATA_URL",
    "NetatmoTileSource",
    "SinglePageSource",
    "StationCatalogBootstrap",
    "geographic_tiles",
    "load_wunderground_seed_stations",
    "parse_metar_station_cache",
    "parse_netatmo_public_stations",
]
