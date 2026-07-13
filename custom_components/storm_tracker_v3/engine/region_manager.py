"""Dynamische runtime-regio's voor Storm Tracker V3.

Een regio is geen land en de observatiehorizon is niet de sharing distance.
De manager bezit de StormEngine/OFE-combinaties; providercontrollers kunnen via
de lifecycle callbacks per runtime-regio starten en stoppen.
"""
from __future__ import annotations

import inspect
import logging
import math
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional

from .observation_fusion_engine import ObservationFusionEngine
from .storm_engine import StormEngine

_LOGGER = logging.getLogger(__name__)

DEFAULT_ENGINE_SHARING_DISTANCE_KM = 150.0
DEFAULT_OBSERVATION_RADIUS_KM = 350.0


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius = 6371.0088
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    value = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return radius * 2 * math.atan2(math.sqrt(value), math.sqrt(1 - value))


@dataclass
class RegionEngine:
    """Eén actieve meteorologische runtime-context."""

    engine_id: str
    center_lat: float
    center_lon: float
    observation_radius_km: float
    storm_engine: StormEngine
    ofe: ObservationFusionEngine
    projection_targets: set[str] = field(default_factory=set)
    target_locations: dict[str, tuple[float, float]] = field(default_factory=dict)
    runtime: object | None = None

    @property
    def storage_key(self) -> str:
        """Stabiele geografische sleutel voor restart-herstel."""
        return f"{self.center_lat:.2f},{self.center_lon:.2f},{self.observation_radius_km:.0f}"

    def accepts_observation(self, lat: float, lon: float) -> bool:
        return _haversine(self.center_lat, self.center_lon, lat, lon) <= self.observation_radius_km


LifecycleCallback = Callable[[RegionEngine], object | Awaitable[object]]


class StormManager:
    """Single source of truth voor alle actieve RegionEngines en targets."""

    def __init__(
        self,
        hass=None,
        *,
        sharing_distance_km: float = DEFAULT_ENGINE_SHARING_DISTANCE_KM,
        observation_radius_km: float = DEFAULT_OBSERVATION_RADIUS_KM,
        on_engine_created: Optional[LifecycleCallback] = None,
        on_engine_removed: Optional[LifecycleCallback] = None,
    ) -> None:
        self._hass = hass
        self.sharing_distance_km = sharing_distance_km
        self.observation_radius_km = observation_radius_km
        self._on_engine_created = on_engine_created
        self._on_engine_removed = on_engine_removed
        self._engines: dict[str, RegionEngine] = {}
        self._target_engine: dict[str, str] = {}
        self._next_engine_number = 1

    def assign_target(self, target_id: str, lat: float, lon: float) -> RegionEngine:
        """Koppel een target aan de dichtstbijzijnde deelbare runtime-regio."""
        old = self.get_engine_for_target(target_id)
        if old and _haversine(old.center_lat, old.center_lon, lat, lon) <= self.sharing_distance_km:
            old.projection_targets.add(target_id)
            old.target_locations[target_id] = (lat, lon)
            return old

        candidate = self._find_shareable(lat, lon)
        if old is not None and old is not candidate:
            self.release(target_id)
        if candidate is None:
            candidate = self._create_engine(lat, lon)
        candidate.projection_targets.add(target_id)
        candidate.target_locations[target_id] = (lat, lon)
        self._target_engine[target_id] = candidate.engine_id
        return candidate

    def release(self, target_id: str) -> Optional[RegionEngine]:
        engine_id = self._target_engine.pop(target_id, None)
        engine = self._engines.get(engine_id) if engine_id else None
        if engine is None:
            return None
        engine.projection_targets.discard(target_id)
        engine.target_locations.pop(target_id, None)
        if not engine.projection_targets:
            self._engines.pop(engine.engine_id, None)
            self._invoke(self._on_engine_removed, engine)
            _LOGGER.info("RegionEngine %s verwijderd: geen targets", engine.engine_id)
        return engine

    def get_engine_for_target(self, target_id: str) -> Optional[RegionEngine]:
        return self._engines.get(self._target_engine.get(target_id, ""))

    def get_all_engines(self) -> list[RegionEngine]:
        return list(self._engines.values())

    def route_observation(self, observation) -> int:
        """Routeer globale pushdata éénmaal naar elke relevante engine."""
        routed = 0
        for engine in self._engines.values():
            if engine.accepts_observation(observation.lat, observation.lon):
                self._schedule(engine.ofe.add_observation(observation))
                routed += 1
        return routed

    def _find_shareable(self, lat: float, lon: float) -> Optional[RegionEngine]:
        candidates = [
            engine for engine in self._engines.values()
            if _haversine(engine.center_lat, engine.center_lon, lat, lon)
            <= self.sharing_distance_km
        ]
        return min(
            candidates,
            key=lambda engine: _haversine(engine.center_lat, engine.center_lon, lat, lon),
            default=None,
        )

    def _create_engine(self, lat: float, lon: float) -> RegionEngine:
        engine_id = f"region-{self._next_engine_number}"
        self._next_engine_number += 1
        storm_engine = StormEngine()

        async def on_batch(observations: list) -> None:
            scoped = [
                observation for observation in observations
                if _haversine(lat, lon, observation.lat, observation.lon)
                <= self.observation_radius_km
            ]
            await storm_engine.process_batch(scoped)

        engine = RegionEngine(
            engine_id=engine_id,
            center_lat=lat,
            center_lon=lon,
            observation_radius_km=self.observation_radius_km,
            storm_engine=storm_engine,
            ofe=ObservationFusionEngine(on_batch=on_batch),
        )
        self._engines[engine_id] = engine
        self._invoke(self._on_engine_created, engine)
        _LOGGER.info(
            "RegionEngine %s aangemaakt op %.4f,%.4f (sharing %.0f km; observatie %.0f km)",
            engine_id, lat, lon, self.sharing_distance_km, self.observation_radius_km,
        )
        return engine

    def _invoke(self, callback: Optional[LifecycleCallback], engine: RegionEngine) -> None:
        if callback is None:
            return
        result = callback(engine)
        if inspect.isawaitable(result):
            self._schedule(result)

    def _schedule(self, awaitable) -> None:
        if self._hass is not None:
            self._hass.async_create_task(awaitable)
            return
        import asyncio
        asyncio.get_running_loop().create_task(awaitable)
