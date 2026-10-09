"""Local station-catalog planning for bounded live ground polling."""

from __future__ import annotations

from dataclasses import replace
import math
from typing import Any, Iterable

from .thin_provider_catalog import ThinProviderRequest
from .thin_station_catalog import (
    GroundStation,
    StationSelectionRequest,
)


GROUND_PROVIDERS = frozenset({"metar", "netatmo", "wunderground"})
GROUND_CAPABILITIES = (
    "rain_rate",
    "pressure",
    "wind_speed",
    "wind_direction",
)
GROUND_MEASUREMENT_GROUPS = (
    ("rain", frozenset({"rain_rate", "rain_accumulation"}), 3),
    ("wind", frozenset({"wind_speed", "wind_direction", "wind_gust"}), 2),
    ("pressure", frozenset({"pressure"}), 2),
)
_EARTH_RADIUS_KM = 6_371.0088


def _distance_km(
    first_latitude: float,
    first_longitude: float,
    second_latitude: float,
    second_longitude: float,
) -> float:
    first_latitude_radians = math.radians(first_latitude)
    second_latitude_radians = math.radians(second_latitude)
    latitude_delta = second_latitude_radians - first_latitude_radians
    longitude_delta = math.radians(second_longitude - first_longitude)
    value = (
        math.sin(latitude_delta / 2.0) ** 2
        + math.cos(first_latitude_radians)
        * math.cos(second_latitude_radians)
        * math.sin(longitude_delta / 2.0) ** 2
    )
    return _EARTH_RADIUS_KM * 2.0 * math.atan2(
        math.sqrt(value),
        math.sqrt(1.0 - value),
    )


class GroundStationRuntime:
    """Turn shared acquisition regions into one deduplicated station plan."""

    def __init__(
        self,
        catalog: Any,
        *,
        required_capabilities: Iterable[str] = GROUND_CAPABILITIES,
        maximum_per_region: int | None = None,
        maximum_station_age_seconds: float | None = None,
        enabled_providers: Iterable[str] = GROUND_PROVIDERS,
    ) -> None:
        self._catalog = catalog
        self._required_capabilities = frozenset(required_capabilities)
        self._maximum_per_region = (
            None if maximum_per_region is None else int(maximum_per_region)
        )
        if self._maximum_per_region is not None and self._maximum_per_region < 1:
            raise ValueError("maximum_per_region_invalid")
        self._maximum_station_age_seconds = (
            None
            if maximum_station_age_seconds is None
            else float(maximum_station_age_seconds)
        )
        self._enabled_providers = frozenset(str(item) for item in enabled_providers)

    def _selection_requests(
        self,
        requests: tuple[ThinProviderRequest, ...],
    ) -> tuple[StationSelectionRequest, ...]:
        selections: dict[str, StationSelectionRequest] = {}
        for request in requests:
            if request.provider_id not in GROUND_PROVIDERS:
                continue
            if request.regions:
                for region in request.regions:
                    for target in region.targets:
                        selection_id = target.target_id
                        selections.setdefault(selection_id, StationSelectionRequest(
                            region_id=selection_id,
                            latitude=target.latitude,
                            longitude=target.longitude,
                            radius_km=request.ground_radius_km,
                            required_capabilities=tuple(self._required_capabilities),
                        ))
            else:
                for target in request.targets:
                    selections.setdefault(target.target_id, StationSelectionRequest(
                        region_id=target.target_id,
                        latitude=target.latitude,
                        longitude=target.longitude,
                        radius_km=request.ground_radius_km,
                        required_capabilities=tuple(self._required_capabilities),
                    ))
        return tuple(selections[key] for key in sorted(selections))

    def _station_is_current(self, station: GroundStation, *, now: float) -> bool:
        return (
            station.operational
            and (
                self._maximum_station_age_seconds is None
                or now - station.last_seen_at <= self._maximum_station_age_seconds
            )
        )

    def _selected_station_ids(
        self,
        inventory: Iterable[GroundStation],
        selections: tuple[StationSelectionRequest, ...],
        *,
        provider_id: str,
        now: float,
    ) -> tuple[str, ...]:
        selected: dict[str, GroundStation] = {}
        provider_inventory = tuple(
            station
            for station in inventory
            if station.provider_id == provider_id
            and self._station_is_current(station, now=now)
        )
        for selection in selections:
            ranked: list[tuple[float, float, float, str, GroundStation]] = []
            for station in provider_inventory:
                distance = _distance_km(
                    selection.latitude,
                    selection.longitude,
                    station.latitude,
                    station.longitude,
                )
                if distance > selection.radius_km:
                    continue
                ranked.append((
                    distance,
                    -station.quality_score,
                    max(0.0, now - station.last_seen_at),
                    station.station_key,
                    station,
                ))
            ranked.sort(key=lambda item: item[:4])
            target_selected: dict[str, GroundStation] = {}
            for _name, capabilities, limit in GROUND_MEASUREMENT_GROUPS:
                if not capabilities.intersection(self._required_capabilities):
                    continue
                matches = (
                    item[4]
                    for item in ranked
                    if capabilities.intersection(item[4].capabilities)
                )
                for station in tuple(matches)[:limit]:
                    target_selected[station.station_key] = station
            if self._maximum_per_region is not None:
                target_selected = dict(tuple(target_selected.items())[:self._maximum_per_region])
            selected.update(target_selected)
        return tuple(sorted(station.station_id for station in selected.values()))

    def prepare(
        self,
        requests: tuple[ThinProviderRequest, ...],
        *,
        now: float,
    ) -> tuple[ThinProviderRequest, ...]:
        selections = self._selection_requests(requests)
        if not selections:
            return requests
        inventory: dict[str, GroundStation] = {}
        try:
            for selection in selections:
                for station in self._catalog.find_nearby(
                    latitude=selection.latitude,
                    longitude=selection.longitude,
                    radius_km=selection.radius_km,
                ):
                    if station.provider_id in self._enabled_providers:
                        inventory[station.station_key] = station
        except Exception:
            return tuple(
                replace(
                    request,
                    selected_station_ids=(),
                    station_selection_applied=True,
                    station_selection_reason="station_catalog_unavailable",
                )
                if request.provider_id in GROUND_PROVIDERS
                else request
                for request in requests
            )
        ids_by_provider = {
            provider_id: self._selected_station_ids(
                inventory.values(),
                selections,
                provider_id=provider_id,
                now=now,
            )
            for provider_id in self._enabled_providers
        }
        selections_by_target = {
            selection.region_id: selection for selection in selections
        }
        return tuple(
            replace(
                request,
                selected_station_ids=ids_by_provider.get(request.provider_id, ()),
                station_selection_applied=True,
                station_selection_reason=(
                    "station_catalog_no_match"
                    if request.provider_id in self._enabled_providers
                    and not ids_by_provider.get(request.provider_id)
                    else None
                ),
                station_discovery_target_ids=tuple(sorted(
                    target.target_id
                    for target in request.targets
                    if request.provider_id in self._enabled_providers
                    and target.target_id in selections_by_target
                    and not self._selected_station_ids(
                        inventory.values(),
                        (selections_by_target[target.target_id],),
                        provider_id=request.provider_id,
                        now=now,
                    )
                )),
            )
            if request.provider_id in GROUND_PROVIDERS
            else request
            for request in requests
        )


__all__ = ["GROUND_CAPABILITIES", "GROUND_PROVIDERS", "GroundStationRuntime"]
