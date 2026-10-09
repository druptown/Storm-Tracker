"""Local station-catalog planning for bounded live ground polling."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, Iterable

from .thin_provider_catalog import ThinProviderRequest
from .thin_station_catalog import (
    GroundStation,
    StationSelectionRequest,
    select_stations,
)


GROUND_PROVIDERS = frozenset({"metar", "netatmo", "wunderground"})
GROUND_CAPABILITIES = (
    "rain_rate",
    "pressure",
    "wind_speed",
    "wind_direction",
)


class GroundStationRuntime:
    """Turn shared acquisition regions into one deduplicated station plan."""

    def __init__(
        self,
        catalog: Any,
        *,
        required_capabilities: Iterable[str] = GROUND_CAPABILITIES,
        maximum_per_region: int = 4,
        maximum_station_age_seconds: float | None = None,
        enabled_providers: Iterable[str] = GROUND_PROVIDERS,
    ) -> None:
        self._catalog = catalog
        self._required_capabilities = tuple(required_capabilities)
        self._maximum_per_region = int(maximum_per_region)
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
                    selections.setdefault(region.region_id, StationSelectionRequest(
                        region_id=region.region_id,
                        latitude=region.center_latitude,
                        longitude=region.center_longitude,
                        radius_km=request.ground_radius_km,
                        required_capabilities=self._required_capabilities,
                    ))
            else:
                for target in request.targets:
                    selections.setdefault(target.target_id, StationSelectionRequest(
                        region_id=target.target_id,
                        latitude=target.latitude,
                        longitude=target.longitude,
                        radius_km=request.ground_radius_km,
                        required_capabilities=self._required_capabilities,
                    ))
        return tuple(selections[key] for key in sorted(selections))

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
        plan = select_stations(
            inventory.values(),
            selections,
            now=now,
            maximum_per_region=self._maximum_per_region,
            maximum_station_age_seconds=self._maximum_station_age_seconds,
        )
        ids_by_provider: dict[str, set[str]] = {}
        for station in plan.stations:
            ids_by_provider.setdefault(station.provider_id, set()).add(station.station_id)
        return tuple(
            replace(
                request,
                selected_station_ids=tuple(sorted(ids_by_provider.get(request.provider_id, ()))),
                station_selection_applied=True,
            )
            if request.provider_id in GROUND_PROVIDERS
            else request
            for request in requests
        )


__all__ = ["GROUND_CAPABILITIES", "GROUND_PROVIDERS", "GroundStationRuntime"]
