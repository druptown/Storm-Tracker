"""Storm Tracker V3 — engine/region_manager.py v0.5.0

Module 4: StormManager (dynamische RegionEngines)

Kernprincipe (architectuurdocument 'Dynamische RegionEngines'):
  Een RegionEngine is een RUNTIME-object — geen vaste definitie per land.
  Hij vertegenwoordigt een geografisch gebied dat nodig is om één of meer
  ProjectionTargets correct te monitoren. Er zijn GEEN vooraf gedefinieerde
  regio's. De StormManager bepaalt volledig automatisch:

  1. Of een bestaande RegionEngine de locatie van een ProjectionTarget
     voldoende dekt (target binnen dekkingsradius van engine).
  2. Als nee: nieuwe RegionEngine aanmaken gecentreerd op de target-locatie.
  3. Providers bepalen ZELF of ze een bepaald gebied ondersteunen via
     `provider.supports(center_lat, center_lon, radius_km)`.
     De StormManager hoeft nooit te weten welke provider bij welk land hoort.

Lifecycle:
  - RegionEngine aanmaken: eerste ProjectionTarget buiten alle bestaande engines
  - RegionEngine afbreken: laatste ProjectionTarget verlaat de engine
    (timeout → providers stoppen → geheugen vrijgeven)

Versiegeschiedenis:
  v0.5.0 — volledig dynamische RegionEngines zonder vaste landsgrenzen;
           providers bepalen zelf hun dekking via supports();
           StormManager is volledig generiek
  v0.4.0 — ProjectionTarget-terminologie doorgevoerd
  v0.3.0 — regio's hebben één canonieke blitz_entity (niet meer geldig)
  v0.2.0 — geografische regio's op basis van land (niet meer geldig)
  v0.1.0 — eerste opzet (regio = blitz_entity, verouderd)
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Callable, Optional

from homeassistant.core import HomeAssistant

from .storm_engine import StormEngine
from .observation_fusion_engine import ObservationFusionEngine

_LOGGER = logging.getLogger(__name__)

# Standaard dekkingsradius per RegionEngine in km.
# 700km dekt ruim een groot land + buurlanden — genoeg om
# stormen die vanuit een aangrenzend gebied naderen tijdig te zien.
DEFAULT_REGION_RADIUS_KM = 700.0


def _haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Afstand in km (Haversine)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@dataclass
class RegionEngine:
    """
    Één dynamische RegionEngine — een geografisch dekkingsgebied.

    Weet niets van landen, grenzen of providers-per-land.
    Bevat uitsluitend runtime-state:
      - center: het geografische middelpunt
      - radius_km: dekkingsstraal
      - actieve providers
      - Observation Fusion Engine (OFE)
      - Storm Engine (WeatherSystem repository)
      - geregistreerde ProjectionTargets + update-callbacks
    """
    engine_id:     str              # uniek label, bv. "engine@51.03,4.48"
    center_lat:    float
    center_lon:    float
    radius_km:     float
    storm_engine:  StormEngine
    ofe:           ObservationFusionEngine
    providers:     list = field(default_factory=list)   # actieve provider-instanties

    # Bewust gescheiden: 'projection_targets' bepaalt de lifecycle van de engine;
    # 'update_callbacks' zijn optioneel en sturen updates naar de coordinator.
    projection_targets: set[str] = field(default_factory=set)
    update_callbacks:   dict[str, Callable] = field(default_factory=dict)

    def covers(self, lat: float, lon: float) -> bool:
        """True als deze locatie binnen de dekkingsradius valt."""
        return _haversine(self.center_lat, self.center_lon, lat, lon) <= self.radius_km


class StormManager:
    """
    Enige instantie (via hass.data) die volledig automatisch:
      1. Bepaalt welke RegionEngine bij een ProjectionTarget-locatie past.
      2. Nieuwe RegionEngines aanmaakt als geen bestaande engine de locatie dekt.
      3. RegionEngines afbreekt zodra ze geen ProjectionTargets meer hebben.

    Providers worden globaal geregistreerd via register_provider().
    De StormManager vraagt elke provider: "dek jij dit gebied?"
    via provider.supports(center_lat, center_lon, radius_km).
    De StormManager hoeft NOOIT te weten welke provider bij welk land hoort.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        region_radius_km: float = DEFAULT_REGION_RADIUS_KM,
    ) -> None:
        self._hass          = hass
        self._region_radius = region_radius_km
        self._engines:      list[RegionEngine] = []
        self._target_engine: dict[str, str] = {}     # target_id → engine_id
        self._provider_factories: list = []           # globaal geregistreerde providers

    def register_provider(self, provider_factory) -> None:
        """
        Registreer een provider-factory globaal.

        Een provider-factory is een callable die (hass, center_lat, center_lon,
        radius_km) ontvangt en een gematerialiseerde provider-instantie terugstuurt,
        of None als de provider dat gebied niet ondersteunt.

        Voorbeeld:
            storm_manager.register_provider(BlitzortungProviderFactory(entity_id))
            storm_manager.register_provider(KmiProviderFactory())
            storm_manager.register_provider(RainViewerProviderFactory())
            storm_manager.register_provider(NetatmoProviderFactory(token_manager))
        """
        self._provider_factories.append(provider_factory)

    def assign_target(
        self,
        projection_target_id: str,
        lat: float,
        lon: float,
        on_storms_updated: Optional[Callable] = None,
        **storm_engine_kwargs,
    ) -> Optional[RegionEngine]:
        """
        Koppel een ProjectionTarget aan een geschikte RegionEngine.

        Stap 1: Zoek een bestaande engine die de locatie dekt.
        Stap 2: Indien geen: maak een nieuwe engine gecentreerd op de locatie.
        Stap 3: Als target al in een andere engine zat: ontkoppel die eerst.

        Geeft de toegewezen RegionEngine terug.
        """
        # Zoek bestaande engine die de locatie dekt
        covering_engine = self._find_covering_engine(lat, lon)

        old_engine_id = self._target_engine.get(projection_target_id)

        # Zelfde engine als voorheen: gewoon callback bijwerken indien nodig
        if covering_engine is not None and old_engine_id == covering_engine.engine_id:
            covering_engine.projection_targets.add(projection_target_id)
            if on_storms_updated is not None:
                covering_engine.update_callbacks[projection_target_id] = on_storms_updated
                self._rebuild_dispatcher(covering_engine)
            return covering_engine

        # Target verhuist naar andere (of nieuwe) engine
        if old_engine_id is not None:
            old_engine = self._get_engine(old_engine_id)
            if old_engine:
                self._leave_engine(projection_target_id, old_engine)
                _LOGGER.info(
                    "ProjectionTarget %s verhuist van %s naar %s",
                    projection_target_id, old_engine_id,
                    covering_engine.engine_id if covering_engine else "nieuwe engine"
                )

        if covering_engine is None:
            covering_engine = self._create_engine(lat, lon, storm_engine_kwargs)

        covering_engine.projection_targets.add(projection_target_id)
        if on_storms_updated is not None:
            covering_engine.update_callbacks[projection_target_id] = on_storms_updated
            self._rebuild_dispatcher(covering_engine)

        self._target_engine[projection_target_id] = covering_engine.engine_id
        return covering_engine

    def release(self, projection_target_id: str) -> None:
        """ProjectionTarget stopt volledig (config entry unload)."""
        engine_id = self._target_engine.pop(projection_target_id, None)
        engine = self._get_engine(engine_id) if engine_id else None
        if engine:
            self._leave_engine(projection_target_id, engine)

    def get_engine_for_target(
        self, projection_target_id: str
    ) -> Optional[RegionEngine]:
        engine_id = self._target_engine.get(projection_target_id)
        return self._get_engine(engine_id) if engine_id else None

    def get_all_engines(self) -> list[RegionEngine]:
        return list(self._engines)

    # ── Interne engine-beheer ─────────────────────────────────────────────

    def _find_covering_engine(self, lat: float, lon: float) -> Optional[RegionEngine]:
        """
        Zoek een bestaande RegionEngine die deze locatie dekt.
        Bij meerdere kandidaten: kies de engine waarvan het centrum
        het dichtst bij de doellocatie ligt (meest relevante data).
        """
        candidates = [e for e in self._engines if e.covers(lat, lon)]
        if not candidates:
            return None
        return min(candidates,
                   key=lambda e: _haversine(e.center_lat, e.center_lon, lat, lon))

    def _get_engine(self, engine_id: str) -> Optional[RegionEngine]:
        for e in self._engines:
            if e.engine_id == engine_id:
                return e
        return None

    def _create_engine(
        self, center_lat: float, center_lon: float, storm_engine_kwargs: dict
    ) -> RegionEngine:
        """
        Maak een nieuwe RegionEngine gecentreerd op (center_lat, center_lon).
        Vraag elke geregistreerde provider of hij dit gebied ondersteunt.
        """
        engine_id = f"engine@{center_lat:.2f},{center_lon:.2f}"

        storm_engine = StormEngine(**storm_engine_kwargs)

        async def _on_observation_batch(observations):
            await storm_engine.process_batch(observations)

        ofe = ObservationFusionEngine(on_batch=_on_observation_batch)

        # Vraag elke provider-factory of hij dit gebied ondersteunt
        active_providers = []
        for factory in self._provider_factories:
            provider = factory.create(
                self._hass, center_lat, center_lon, self._region_radius
            )
            if provider is not None:
                def _make_obs_callback(p=provider):
                    def _on_obs(obs):
                        self._hass.async_create_task(ofe.add_observation(obs))
                    return _on_obs
                provider.set_callback(_make_obs_callback())
                provider.start()
                active_providers.append(provider)
                _LOGGER.debug(
                    "Provider %s gestart voor engine %s",
                    provider.__class__.__name__, engine_id
                )

        region = RegionEngine(
            engine_id=engine_id,
            center_lat=center_lat,
            center_lon=center_lon,
            radius_km=self._region_radius,
            storm_engine=storm_engine,
            ofe=ofe,
            providers=active_providers,
        )
        self._engines.append(region)
        _LOGGER.info(
            "Nieuwe RegionEngine aangemaakt: %s (%.0f km, %d providers)",
            engine_id, self._region_radius, len(active_providers)
        )
        return region

    def _leave_engine(
        self, projection_target_id: str, engine: RegionEngine
    ) -> None:
        """Verwijder een ProjectionTarget uit een engine; breek af als leeg."""
        engine.projection_targets.discard(projection_target_id)
        engine.update_callbacks.pop(projection_target_id, None)
        self._rebuild_dispatcher(engine)

        if not engine.projection_targets:
            for provider in engine.providers:
                try:
                    provider.stop()
                except Exception:
                    pass
            self._engines = [e for e in self._engines if e.engine_id != engine.engine_id]
            _LOGGER.info(
                "RegionEngine %s afgebroken (geen ProjectionTargets meer)",
                engine.engine_id
            )

    def _rebuild_dispatcher(self, engine: RegionEngine) -> None:
        """Stel storm_engine callback in op dispatcher naar alle update_callbacks."""
        def _dispatch(weather_systems):
            for cb in list(engine.update_callbacks.values()):
                try:
                    cb(weather_systems)
                except Exception:
                    _LOGGER.exception("Fout in WeatherSystem update-callback")
        engine.storm_engine._on_updated = _dispatch
