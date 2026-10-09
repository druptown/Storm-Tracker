"""Provider-neutral metadata catalog for nearby ground weather stations."""

from __future__ import annotations

from dataclasses import dataclass
import math
import re
from types import MappingProxyType
from typing import Any, Callable, Iterable, Mapping


_PROVIDER_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_CAPABILITY_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_CAPABILITIES = frozenset({
    "rain_rate",
    "rain_accumulation",
    "temperature",
    "relative_humidity",
    "pressure",
    "wind_speed",
    "wind_gust",
    "wind_direction",
})
_EARTH_RADIUS_KM = 6_371.0088


def normalize_capabilities(values: Iterable[str]) -> tuple[str, ...]:
    normalized: set[str] = set()
    for value in values:
        item = str(value).strip().lower()
        if item not in _CAPABILITIES or _CAPABILITY_RE.fullmatch(item) is None:
            raise ValueError("station_capability_invalid")
        normalized.add(item)
    return tuple(sorted(normalized))


def _finite(value: object, label: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{label}_invalid") from error
    if not math.isfinite(result):
        raise ValueError(f"{label}_invalid")
    return result


@dataclass(frozen=True, slots=True)
class GroundStation:
    provider_id: str
    station_id: str
    latitude: float
    longitude: float
    capabilities: frozenset[str]
    discovered_at: float
    last_seen_at: float
    quality_score: float
    operational: bool = True

    @classmethod
    def create(
        cls,
        *,
        provider_id: str,
        station_id: str,
        latitude: float,
        longitude: float,
        capabilities: Iterable[str],
        discovered_at: float,
        last_seen_at: float,
        quality_score: float = 1.0,
        operational: bool = True,
    ) -> "GroundStation":
        provider = str(provider_id).strip().lower()
        identifier = str(station_id).strip()
        if _PROVIDER_RE.fullmatch(provider) is None:
            raise ValueError("station_provider_invalid")
        if not identifier or len(identifier) > 191 or any(
            ord(character) < 32 for character in identifier
        ):
            raise ValueError("station_id_invalid")
        lat = _finite(latitude, "station_latitude")
        lon = _finite(longitude, "station_longitude")
        if not -90.0 <= lat <= 90.0 or not -180.0 <= lon <= 180.0:
            raise ValueError("station_coordinates_invalid")
        discovered = _finite(discovered_at, "station_discovered_at")
        last_seen = _finite(last_seen_at, "station_last_seen_at")
        quality = _finite(quality_score, "station_quality_score")
        if discovered < 0.0 or last_seen < 0.0 or not 0.0 <= quality <= 1.0:
            raise ValueError("station_metadata_invalid")
        return cls(
            provider_id=provider,
            station_id=identifier,
            latitude=lat,
            longitude=lon,
            capabilities=frozenset(normalize_capabilities(capabilities)),
            discovered_at=discovered,
            last_seen_at=last_seen,
            quality_score=quality,
            operational=bool(operational),
        )

    @property
    def station_key(self) -> str:
        return f"{self.provider_id}:{self.station_id.casefold()}"


@dataclass(frozen=True, slots=True)
class DiscoveryCheckpoint:
    provider_id: str
    tile_id: str
    cursor: str | None
    completed: bool
    station_count: int
    updated_at: float

    def __post_init__(self) -> None:
        if _PROVIDER_RE.fullmatch(self.provider_id) is None:
            raise ValueError("checkpoint_provider_invalid")
        if not self.tile_id or len(self.tile_id) > 191:
            raise ValueError("checkpoint_tile_invalid")
        if self.cursor is not None and len(self.cursor) > 1024:
            raise ValueError("checkpoint_cursor_invalid")
        if isinstance(self.station_count, bool) or self.station_count < 0:
            raise ValueError("checkpoint_count_invalid")
        if _finite(self.updated_at, "checkpoint_updated_at") < 0.0:
            raise ValueError("checkpoint_updated_at_invalid")

    @property
    def resume_cursor(self) -> str | None:
        return None if self.completed else self.cursor

    def needs_refresh(self, *, now: float, maximum_age_seconds: float) -> bool:
        current = _finite(now, "checkpoint_now")
        maximum_age = _finite(maximum_age_seconds, "checkpoint_maximum_age")
        if maximum_age < 0.0:
            raise ValueError("checkpoint_maximum_age_invalid")
        return not self.completed or current - self.updated_at > maximum_age


@dataclass(frozen=True, slots=True)
class StationSelectionRequest:
    region_id: str
    latitude: float
    longitude: float
    radius_km: float
    required_capabilities: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.region_id:
            raise ValueError("station_region_invalid")
        if not -90.0 <= _finite(self.latitude, "region_latitude") <= 90.0:
            raise ValueError("region_coordinates_invalid")
        if not -180.0 <= _finite(self.longitude, "region_longitude") <= 180.0:
            raise ValueError("region_coordinates_invalid")
        if _finite(self.radius_km, "region_radius") <= 0.0:
            raise ValueError("region_radius_invalid")
        object.__setattr__(
            self,
            "required_capabilities",
            normalize_capabilities(self.required_capabilities),
        )


@dataclass(frozen=True, slots=True)
class StationSelectionPlan:
    regions: Mapping[str, tuple[GroundStation, ...]]
    stations: tuple[GroundStation, ...]


def _distance_km(
    first_latitude: float,
    first_longitude: float,
    second_latitude: float,
    second_longitude: float,
) -> float:
    first_lat = math.radians(first_latitude)
    second_lat = math.radians(second_latitude)
    delta_lat = second_lat - first_lat
    delta_lon = math.radians(second_longitude - first_longitude)
    a = (
        math.sin(delta_lat / 2.0) ** 2
        + math.cos(first_lat) * math.cos(second_lat)
        * math.sin(delta_lon / 2.0) ** 2
    )
    return _EARTH_RADIUS_KM * 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _direction_sector(
    origin_latitude: float,
    origin_longitude: float,
    latitude: float,
    longitude: float,
) -> int:
    north = latitude - origin_latitude
    east = (longitude - origin_longitude) * math.cos(math.radians(origin_latitude))
    bearing = math.degrees(math.atan2(east, north)) % 360.0
    return int((bearing + 45.0) // 90.0) % 4


def select_stations(
    stations: Iterable[GroundStation],
    requests: Iterable[StationSelectionRequest],
    *,
    now: float,
    maximum_per_region: int = 4,
    maximum_station_age_seconds: float = 7 * 86400.0,
) -> StationSelectionPlan:
    current = _finite(now, "selection_now")
    if maximum_per_region < 1:
        raise ValueError("maximum_per_region_invalid")
    inventory = {item.station_key: item for item in stations}
    combined: dict[str, StationSelectionRequest] = {}
    for request in requests:
        previous = combined.get(request.region_id)
        if previous is None:
            combined[request.region_id] = request
            continue
        combined[request.region_id] = StationSelectionRequest(
            region_id=request.region_id,
            latitude=previous.latitude,
            longitude=previous.longitude,
            radius_km=max(previous.radius_km, request.radius_km),
            required_capabilities=tuple(sorted(
                set(previous.required_capabilities)
                | set(request.required_capabilities)
            )),
        )

    selected_by_region: dict[str, tuple[GroundStation, ...]] = {}
    all_selected: dict[str, GroundStation] = {}
    for region_id, request in sorted(combined.items()):
        candidates: list[tuple[float, GroundStation]] = []
        for station in inventory.values():
            if (
                not station.operational
                or current - station.last_seen_at > maximum_station_age_seconds
            ):
                continue
            distance = _distance_km(
                request.latitude,
                request.longitude,
                station.latitude,
                station.longitude,
            )
            if distance <= request.radius_km:
                candidates.append((distance, station))
        candidates.sort(key=lambda item: (
            item[0], -item[1].quality_score, item[1].station_key,
        ))

        remaining = set(request.required_capabilities)
        chosen: list[GroundStation] = []
        selected_sectors: set[int] = set()
        while remaining and len(chosen) < maximum_per_region:
            best: tuple[int, int, float, float, float, str, GroundStation, int] | None = None
            for distance, station in candidates:
                if station in chosen:
                    continue
                coverage = len(remaining.intersection(station.capabilities))
                if coverage == 0:
                    continue
                sector = _direction_sector(
                    request.latitude,
                    request.longitude,
                    station.latitude,
                    station.longitude,
                )
                rank = (
                    -coverage,
                    1 if sector in selected_sectors else 0,
                    distance,
                    max(0.0, current - station.last_seen_at),
                    -station.quality_score,
                    station.station_key,
                    station,
                    sector,
                )
                if best is None or rank[:6] < best[:6]:
                    best = rank
            if best is None:
                break
            station = best[6]
            chosen.append(station)
            selected_sectors.add(best[7])
            remaining.difference_update(station.capabilities)
        selected_by_region[region_id] = tuple(chosen)
        all_selected.update((station.station_key, station) for station in chosen)

    return StationSelectionPlan(
        regions=MappingProxyType(selected_by_region),
        stations=tuple(all_selected[key] for key in sorted(all_selected)),
    )


_UPSERT_STATION = """
INSERT INTO stv3_ground_station(
    provider_id,station_id,position,discovered_at,last_seen_at,
    quality_score,operational
) VALUES(%s,%s,ST_SetSRID(ST_MakePoint(%s,%s),4326),
         to_timestamp(%s),to_timestamp(%s),%s,%s)
ON CONFLICT(provider_id,station_id) DO UPDATE SET
    position=EXCLUDED.position,
    last_seen_at=GREATEST(stv3_ground_station.last_seen_at,EXCLUDED.last_seen_at),
    quality_score=EXCLUDED.quality_score,
    operational=EXCLUDED.operational,
    updated_at=clock_timestamp()
"""

_DELETE_CAPABILITIES = """
DELETE FROM stv3_station_capability
WHERE provider_id=%s AND station_id=%s
"""

_INSERT_CAPABILITY = """
INSERT INTO stv3_station_capability(provider_id,station_id,capability)
VALUES(%s,%s,%s)
ON CONFLICT(provider_id,station_id,capability) DO NOTHING
"""

_UPSERT_CHECKPOINT = """
INSERT INTO stv3_station_discovery_tile(
    provider_id,tile_id,cursor,completed,station_count,updated_at
) VALUES(%s,%s,%s,%s,%s,to_timestamp(%s))
ON CONFLICT(provider_id,tile_id) DO UPDATE SET
    cursor=EXCLUDED.cursor,
    completed=EXCLUDED.completed,
    station_count=EXCLUDED.station_count,
    updated_at=EXCLUDED.updated_at
"""

_SELECT_CHECKPOINT = """
SELECT provider_id,tile_id,cursor,completed,station_count,
       EXTRACT(EPOCH FROM updated_at)
FROM stv3_station_discovery_tile
WHERE provider_id=%s AND tile_id=%s
"""

_SELECT_NEARBY_STATIONS = """
SELECT station.provider_id,station.station_id,
       ST_Y(station.position),ST_X(station.position),
       ARRAY_REMOVE(ARRAY_AGG(capability.capability),NULL),
       EXTRACT(EPOCH FROM station.discovered_at),
       EXTRACT(EPOCH FROM station.last_seen_at),
       station.quality_score,station.operational
FROM stv3_ground_station AS station
LEFT JOIN stv3_station_capability AS capability
  ON capability.provider_id=station.provider_id
 AND capability.station_id=station.station_id
WHERE ST_DWithin(
    station.position::geography,
    ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography,
    %s
)
GROUP BY station.provider_id,station.station_id,station.position,
         station.discovered_at,station.last_seen_at,
         station.quality_score,station.operational
ORDER BY ST_Distance(
    station.position::geography,
    ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography
),station.provider_id,station.station_id
"""


class StationCatalogRepository:
    """Small PostGIS write boundary; API discovery lives elsewhere."""

    def __init__(
        self,
        connect_factory: Callable[..., Any],
        connect_kwargs: Mapping[str, Any],
    ) -> None:
        self._connect_factory = connect_factory
        self._connect_kwargs = dict(connect_kwargs)

    def _write(self, operation: Callable[[Any], None]) -> None:
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                operation(cursor)
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def upsert_stations(self, stations: Iterable[GroundStation]) -> int:
        unique = {station.station_key: station for station in stations}

        def operation(cursor: Any) -> None:
            for station in unique.values():
                cursor.execute(_UPSERT_STATION, (
                    station.provider_id,
                    station.station_id,
                    station.longitude,
                    station.latitude,
                    station.discovered_at,
                    station.last_seen_at,
                    station.quality_score,
                    station.operational,
                ))
                cursor.execute(_DELETE_CAPABILITIES, (
                    station.provider_id, station.station_id,
                ))
                cursor.executemany(
                    _INSERT_CAPABILITY,
                    (
                        (station.provider_id, station.station_id, capability)
                        for capability in sorted(station.capabilities)
                    ),
                )

        self._write(operation)
        return len(unique)

    def save_checkpoint(self, checkpoint: DiscoveryCheckpoint) -> None:
        self._write(lambda cursor: cursor.execute(_UPSERT_CHECKPOINT, (
            checkpoint.provider_id,
            checkpoint.tile_id,
            checkpoint.cursor,
            checkpoint.completed,
            checkpoint.station_count,
            checkpoint.updated_at,
        )))

    def load_checkpoint(
        self,
        provider_id: str,
        tile_id: str,
    ) -> DiscoveryCheckpoint | None:
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(_SELECT_CHECKPOINT, (provider_id, tile_id))
                row = cursor.fetchone()
            if row is None:
                return None
            return DiscoveryCheckpoint(
                provider_id=str(row[0]),
                tile_id=str(row[1]),
                cursor=None if row[2] is None else str(row[2]),
                completed=bool(row[3]),
                station_count=int(row[4]),
                updated_at=float(row[5]),
            )
        finally:
            connection.close()

    def find_nearby(
        self,
        *,
        latitude: float,
        longitude: float,
        radius_km: float,
    ) -> tuple[GroundStation, ...]:
        lat = _finite(latitude, "station_query_latitude")
        lon = _finite(longitude, "station_query_longitude")
        radius = _finite(radius_km, "station_query_radius")
        if not -90.0 <= lat <= 90.0 or not -180.0 <= lon <= 180.0:
            raise ValueError("station_query_coordinates_invalid")
        if radius <= 0.0:
            raise ValueError("station_query_radius_invalid")
        connection = self._connect_factory(**self._connect_kwargs)
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    _SELECT_NEARBY_STATIONS,
                    (lon, lat, radius * 1000.0, lon, lat),
                )
                rows = cursor.fetchall()
            return tuple(GroundStation.create(
                provider_id=str(row[0]),
                station_id=str(row[1]),
                latitude=float(row[2]),
                longitude=float(row[3]),
                capabilities=tuple(row[4] or ()),
                discovered_at=float(row[5]),
                last_seen_at=float(row[6]),
                quality_score=float(row[7]),
                operational=bool(row[8]),
            ) for row in rows)
        finally:
            connection.close()


__all__ = [
    "DiscoveryCheckpoint",
    "GroundStation",
    "StationCatalogRepository",
    "StationSelectionPlan",
    "StationSelectionRequest",
    "normalize_capabilities",
    "select_stations",
]
